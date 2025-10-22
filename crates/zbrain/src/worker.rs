use crate::job::{Job, JobId};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLease {
    pub job_id: JobId,
    pub worker_id: String,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl WorkerLease {
    pub fn new(job_id: JobId, worker_id: String, ttl_secs: u64) -> Self {
        let now = Utc::now();
        Self {
            job_id,
            worker_id,
            acquired_at: now,
            expires_at: now + chrono::Duration::seconds(ttl_secs as i64),
        }
    }
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }
    pub fn renew(&mut self, ttl_secs: u64) {
        self.expires_at = Utc::now() + chrono::Duration::seconds(ttl_secs as i64);
    }
}

#[derive(Debug, Clone)]
pub enum WorkerEvent {
    JobStarted {
        job_id: JobId,
        worker_id: String,
    },
    JobProgress {
        job_id: JobId,
        stdout: Option<String>,
        stderr: Option<String>,
        sequence: u64,
    },
    JobCompleted {
        job_id: JobId,
        output: Option<serde_json::Value>,
        artifacts: Vec<WorkerArtifact>,
    },
    JobFailed {
        job_id: JobId,
        error: String,
    },
    JobCanceled {
        job_id: JobId,
        reason: Option<String>,
    },
}

#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    async fn execute(
        &self,
        job: Job,
        event_tx: mpsc::UnboundedSender<WorkerEvent>,
        cancel: CancellationToken,
    ) -> Result<()>;
    fn id(&self) -> &str;
    async fn health_check(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct WorkerArtifact {
    pub name: String,
    pub path: PathBuf,
}
