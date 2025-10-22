use crate::{
    job::{Job, JobId, JobState},
    storage::{JobArtifact, LogEntry, Storage},
    worker::{WorkerArtifact, WorkerEvent, WorkerLease},
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use serde_json::json;
use sha2::{Digest, Sha256};
use zcommon::metrics::JobMetrics;
use std::{
    collections::HashMap,
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{broadcast, mpsc, RwLock},
    time::interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
// use zobservability::ZdbMetrics; // temporarily disabled due to cyclic dependency

pub struct JobQueue {
    storage: Storage,
    leases: Arc<RwLock<HashMap<JobId, WorkerLease>>>,
    cancel_tokens: Arc<RwLock<HashMap<JobId, CancellationToken>>>,
    #[allow(dead_code)]
    event_tx: mpsc::UnboundedSender<WorkerEvent>,
    pub lease_ttl_secs: u64,
    bus: broadcast::Sender<BusEvent>,
    seq: AtomicU64,
    artifact_root: PathBuf,
    metrics: Option<Arc<dyn JobMetrics>>,
}

impl JobQueue {
    pub fn new(
        storage: Storage,
        artifact_root: impl Into<PathBuf>,
    ) -> (Self, mpsc::UnboundedReceiver<WorkerEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (bus, _) = broadcast::channel(1024);
        let artifact_root = artifact_root.into();
        let _ = std::fs::create_dir_all(&artifact_root);
        (
            Self {
                storage,
                leases: Arc::new(RwLock::new(HashMap::new())),
                cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
                event_tx: tx,
                lease_ttl_secs: 300,
                bus,
                seq: AtomicU64::new(1),
                artifact_root,
                metrics: None,
            },
            rx,
        )
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn JobMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn publish(&self, topic: &str, data: serde_json::Value) {
        let ev = BusEvent {
            topic: topic.to_string(),
            seq: self.seq.fetch_add(1, Ordering::SeqCst),
            ts: Utc::now(),
            data,
        };
        let _ = self.bus.send(ev);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BusEvent> {
        self.bus.subscribe()
    }
    pub fn get_logs(&self, job_id: &JobId, from_seq: u64, limit: usize) -> Result<Vec<LogEntry>> {
        self.storage.get_logs(job_id, from_seq, limit)
    }

    pub async fn register_cancel_token(&self, job_id: JobId, token: CancellationToken) {
        self.cancel_tokens.write().await.insert(job_id, token);
    }

    pub async fn clear_cancel_token(&self, job_id: &JobId) {
        self.cancel_tokens.write().await.remove(job_id);
    }

    async fn signal_cancel(&self, job_id: &JobId) -> bool {
        let token = {
            let guard = self.cancel_tokens.read().await;
            guard.get(job_id).cloned()
        };
        if let Some(t) = token {
            t.cancel();
            true
        } else {
            false
        }
    }

    pub fn list_artifacts(&self, job_id: &JobId) -> Result<Vec<JobArtifact>> {
        self.storage.get_artifacts(job_id)
    }

    pub fn artifact_fs_path(&self, org_id: &str, job_id: &JobId, name: &str) -> Option<PathBuf> {
        let rel = Path::new(name);
        if !Self::is_safe_artifact_name(rel) {
            return None;
        }
        Some(
            self.artifact_root
                .join(format!("org_{}", org_id))
                .join(job_id.to_string())
                .join(rel),
        )
    }

    pub async fn cancel_job(&self, job_id: &JobId) -> Result<CancelResult> {
        let Some(mut job) = self.storage.get_job(job_id)? else {
            return Ok(CancelResult::NotFound);
        };
        match job.state {
            JobState::Pending | JobState::Retrying => {
                job.cancel();
                job.error_message
                    .get_or_insert_with(|| "canceled".to_string());
                self.storage.save_job(&job)?;
                self.leases.write().await.remove(job_id);
                self.clear_cancel_token(job_id).await;
                self.publish(
                    &format!("jobs/{}", job_id),
                    json!({"event":"state","status":"canceled"}),
                );
                Ok(CancelResult::PendingCanceled)
            }
            JobState::Running => {
                if self.signal_cancel(job_id).await {
                    self.publish(
                        &format!("jobs/{}", job_id),
                        json!({"event":"state","status":"canceling"}),
                    );
                    Ok(CancelResult::RunningSignal)
                } else {
                    Ok(CancelResult::NoActiveWorker)
                }
            }
            state => Ok(CancelResult::AlreadyFinished(state)),
        }
    }

    async fn persist_artifacts_for_job(
        &self,
        job: &Job,
        artifacts: &[WorkerArtifact],
    ) -> Result<Vec<JobArtifact>> {
        if artifacts.is_empty() {
            self.storage.clear_artifacts(&job.id)?;
            return Ok(Vec::new());
        }
        let job_dir = self
            .artifact_root
            .join(format!("org_{}", job.spec.org_id))
            .join(job.id.to_string());
        fs::create_dir_all(&job_dir).await?;
        let mut manifest = Vec::new();
        for art in artifacts {
            match self.copy_artifact(&job_dir, art).await {
                Ok(item) => manifest.push(item),
                Err(e) => {
                    warn!("artifact {} skipped: {}", art.name, e);
                }
            }
        }
        self.storage.save_artifacts(&job.id, &manifest)?;
        Ok(manifest)
    }

    async fn copy_artifact(&self, job_dir: &Path, art: &WorkerArtifact) -> Result<JobArtifact> {
        if !Self::is_safe_artifact_name(Path::new(&art.name)) {
            return Err(anyhow!("invalid artifact name"));
        }
        let source_meta = fs::metadata(&art.path).await?;
        if !source_meta.is_file() {
            return Err(anyhow!("artifact is not a file"));
        }
        let rel_path = Path::new(&art.name);
        let dest_path = job_dir.join(rel_path);
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut src = fs::File::open(&art.path).await?;
        let mut dest = fs::File::create(&dest_path).await?;
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        let mut buf = [0u8; 8192];
        loop {
            let n = src.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            dest.write_all(&buf[..n]).await?;
            size += n as u64;
        }
        dest.flush().await?;
        let digest = hasher.finalize();
        Ok(JobArtifact {
            name: art.name.clone(),
            sha256: format!("{:x}", digest),
            size,
        })
    }

    fn is_safe_artifact_name(path: &Path) -> bool {
        path.components()
            .all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
    }

    pub async fn submit(&self, job: Job) -> Result<JobId> {
        // Track job submission metrics using abstract interface
        if let Some(metrics) = &self.metrics {
            metrics.record_job_submission(&job.spec.cmd.join(" "), "default");

            // Update queue size metrics
            let queue_size = self.storage.list_jobs(Some(&job.spec.org_id), Some(&JobState::Pending))?.len();
            metrics.record_queue_size("default", "normal", queue_size as u64);
        }

        self.storage.save_job(&job)?;
        info!("job {} submitted to queue", job.id);
        Ok(job.id)
    }
    pub async fn get(&self, id: &JobId) -> Result<Option<Job>> {
        self.storage.get_job(id)
    }
    pub async fn list(&self, org: Option<&str>, st: Option<&JobState>) -> Result<Vec<Job>> {
        self.storage.list_jobs(org, st)
    }

    pub async fn acquire_lease(&self, worker_id: &str, org_id: &str) -> Result<Option<Job>> {
        let jobs = self
            .storage
            .list_jobs(Some(org_id), Some(&JobState::Pending))?;
        for mut j in jobs {
            let leases = self.leases.read().await;
            if let Some(l) = leases.get(&j.id) {
                if !l.is_expired() {
                    continue;
                }
            }
            drop(leases);
            let lease = WorkerLease::new(j.id, worker_id.to_string(), self.lease_ttl_secs);
            j.start(worker_id.to_string());
            self.storage.save_job(&j)?;
            self.leases.write().await.insert(j.id, lease);
            self.publish(
                &format!("jobs/{}", j.id),
                json!({"event":"state","status":"running"}),
            );
            return Ok(Some(j));
        }
        Ok(None)
    }

    pub async fn release_lease(&self, job_id: &JobId, worker_id: &str) -> Result<()> {
        let mut l = self.leases.write().await;
        if let Some(lease) = l.get(job_id) {
            if lease.worker_id == worker_id {
                l.remove(job_id);
            }
        }
        Ok(())
    }

    pub async fn handle_worker_event(&self, ev: WorkerEvent) -> Result<()> {
        match ev {
            WorkerEvent::JobStarted { job_id, worker_id } => {
                debug!("{} started by {}", job_id, worker_id);
            }
            WorkerEvent::JobProgress {
                job_id,
                stdout,
                stderr,
                sequence: _,
            } => {
                if let Some(line) = stdout.as_ref() {
                    let _ = self.storage.append_log(&job_id, "stdout", line);
                    self.publish(
                        &format!("jobs/{}", job_id),
                        json!({"event":"log","stream":"stdout","line":line}),
                    );
                }
                if let Some(line) = stderr.as_ref() {
                    let _ = self.storage.append_log(&job_id, "stderr", line);
                    self.publish(
                        &format!("jobs/{}", job_id),
                        json!({"event":"log","stream":"stderr","line":line}),
                    );
                }
            }
            WorkerEvent::JobCompleted {
                job_id,
                output,
                artifacts,
            } => {
                if let Some(mut j) = self.storage.get_job(&job_id)? {
                    if let Err(e) = self.persist_artifacts_for_job(&j, &artifacts).await {
                        error!("artifact persist {}: {}", job_id, e);
                    }
                    j.succeed(output);
                    self.storage.save_job(&j)?;
                }
                self.leases.write().await.remove(&job_id);
                self.clear_cancel_token(&job_id).await;
                self.publish(
                    &format!("jobs/{}", job_id),
                    json!({"event":"state","status":"succeeded"}),
                );
            }
            WorkerEvent::JobFailed { job_id, error } => {
                if let Some(mut j) = self.storage.get_job(&job_id)? {
                    j.fail(error.clone());
                    if j.state == JobState::Retrying {
                        j.reset_for_retry();
                    }
                    self.storage.save_job(&j)?;
                }
                self.leases.write().await.remove(&job_id);
                self.clear_cancel_token(&job_id).await;
                self.publish(
                    &format!("jobs/{}", job_id),
                    json!({"event":"state","status":"failed","error": error}),
                );
            }
            WorkerEvent::JobCanceled { job_id, reason } => {
                if let Some(mut j) = self.storage.get_job(&job_id)? {
                    j.cancel();
                    if let Some(r) = reason.clone() {
                        j.error_message = Some(r);
                    }
                    self.storage.save_job(&j)?;
                }
                self.leases.write().await.remove(&job_id);
                self.clear_cancel_token(&job_id).await;
                self.publish(
                    &format!("jobs/{}", job_id),
                    json!({"event":"state","status":"canceled","reason": reason}),
                );
            }
        }
        Ok(())
    }

    pub fn start_lease_cleanup(&self) -> tokio::task::JoinHandle<()> {
        let leases = self.leases.clone();
        let storage = self.storage.clone();
        let cancel_tokens = self.cancel_tokens.clone();
        tokio::spawn(async move {
            let mut t = interval(Duration::from_secs(60));
            loop {
                t.tick().await;
                let mut expired = Vec::new();
                {
                    let mut g = leases.write().await;
                    g.retain(|job_id, lease| {
                        if lease.is_expired() {
                            expired.push(*job_id);
                            false
                        } else {
                            true
                        }
                    });
                }
                for id in &expired {
                    if let Ok(Some(mut j)) = storage.get_job(id) {
                        if j.state == JobState::Running {
                            j.state = JobState::Pending;
                            j.worker_id = None;
                            j.started_at = None;
                            j.updated_at = chrono::Utc::now();
                            let _ = storage.save_job(&j);
                        }
                    }
                }
                if !expired.is_empty() {
                    let mut tokens = cancel_tokens.write().await;
                    for id in &expired {
                        tokens.remove(id);
                    }
                }
            }
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BusEvent {
    pub topic: String,
    pub seq: u64,
    pub ts: chrono::DateTime<chrono::Utc>,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelResult {
    NotFound,
    PendingCanceled,
    RunningSignal,
    NoActiveWorker,
    AlreadyFinished(JobState),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobSpec;

    #[tokio::test]
    async fn cancel_pending_job() -> Result<()> {
        let storage = Storage::in_memory()?;
        let tmp = tempfile::tempdir()?;
        let (queue, _rx) = JobQueue::new(storage, tmp.path());
        let job = Job::new(JobSpec {
            org_id: "test".into(),
            cmd: vec!["echo".into(), "hi".into()],
            input: serde_json::Value::Null,
            env: Default::default(),
            timeout_secs: None,
            max_retries: 1,
        });
        let job_id = job.id;
        queue.submit(job).await?;
        let res = queue.cancel_job(&job_id).await?;
        assert_eq!(res, CancelResult::PendingCanceled);
        let updated = queue.get(&job_id).await?.expect("job exists");
        assert_eq!(updated.state, JobState::Canceled);
        Ok(())
    }
}
