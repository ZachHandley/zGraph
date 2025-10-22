use crate::job::{Job, JobId, JobState};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use zcore_storage::{
    Store, ReadTransaction, WriteTransaction,
    COL_JOBS, COL_JOB_LOG_SEQ, COL_JOB_LOGS, COL_JOB_ARTIFACTS
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub seq: u64,
    pub ts: DateTime<Utc>,
    pub stream: String,
    pub line: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobArtifact {
    pub name: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Clone)]
pub struct Storage {
    store: Arc<Store>,
}

impl Storage {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = Store::open(path)?;
        Ok(Self { store: Arc::new(store) })
    }

    pub fn in_memory() -> Result<Self> {
        // For fjall, we'll create a temporary directory for in-memory-like storage
        // Note: fjall doesn't have a pure in-memory mode like redb
        let temp_dir = tempfile::tempdir()?;
        let store = Store::open(temp_dir.path().join("zbrain_temp"))?;
        Ok(Self { store: Arc::new(store) })
    }

    pub fn save_job(&self, job: &Job) -> Result<()> {
        let bytes = serde_json::to_vec(job)?;
        let key = job.id.to_string();

        let mut tx = WriteTransaction::new();
        tx.set(COL_JOBS, key.as_bytes().to_vec(), bytes);
        tx.commit(&self.store)?;
        Ok(())
    }

    pub fn get_job(&self, id: &JobId) -> Result<Option<Job>> {
        let key = id.to_string();
        let tx = ReadTransaction::new();

        if let Some(bytes) = tx.get(&self.store, COL_JOBS, key.as_bytes())? {
            let job: Job = serde_json::from_slice(&bytes)?;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    pub fn list_jobs(&self, org_id: Option<&str>, state: Option<&JobState>) -> Result<Vec<Job>> {
        let tx = ReadTransaction::new();
        let items = tx.scan_prefix(&self.store, COL_JOBS, b"")?; // Empty prefix to get all jobs

        let mut out = Vec::new();
        for (_, value_bytes) in items {
            let j: Job = serde_json::from_slice(&value_bytes)?;
            if let Some(org) = org_id {
                if j.spec.org_id != org {
                    continue;
                }
            }
            if let Some(s) = state {
                if &j.state != s {
                    continue;
                }
            }
            out.push(j);
        }
        out.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(out)
    }

    pub fn delete_job(&self, id: &JobId) -> Result<bool> {
        let key = id.to_string();
        let key_bytes = key.as_bytes();

        let mut tx = WriteTransaction::new();

        // Check if job exists first
        let existed = tx.get(&self.store, COL_JOBS, key_bytes)?.is_some();

        if existed {
            // Delete job
            tx.delete(COL_JOBS, key_bytes.to_vec());

            // Delete associated data
            tx.delete(COL_JOB_LOG_SEQ, key_bytes.to_vec());
            tx.delete(COL_JOB_ARTIFACTS, key_bytes.to_vec());

            // Delete all logs for this job (scan and delete by prefix)
            let log_prefix = format!("{}:", id);
            let tx_read = ReadTransaction::new();
            let log_items = tx_read.scan_prefix(&self.store, COL_JOB_LOGS, log_prefix.as_bytes())?;
            for (log_key, _) in log_items {
                tx.delete(COL_JOB_LOGS, log_key);
            }
        }

        tx.commit(&self.store)?;
        Ok(existed)
    }

    fn log_key(job_id: &JobId, seq: u64) -> String {
        format!("{}:{:020}", job_id, seq)
    }

    pub fn append_log(&self, job_id: &JobId, stream: &str, line: &str) -> Result<LogEntry> {
        let job_key = job_id.to_string();
        let job_key_bytes = job_key.as_bytes();

        let mut tx = WriteTransaction::new();

        // Get current sequence number
        let current_seq = if let Some(seq_bytes) = tx.get(&self.store, COL_JOB_LOG_SEQ, job_key_bytes)? {
            serde_json::from_slice::<u64>(&seq_bytes)?
        } else {
            0
        };

        let next_seq = current_seq + 1;

        // Update sequence number
        let seq_bytes = serde_json::to_vec(&next_seq)?;
        tx.set(COL_JOB_LOG_SEQ, job_key_bytes.to_vec(), seq_bytes);

        // Create log entry
        let entry = LogEntry {
            seq: next_seq,
            ts: Utc::now(),
            stream: stream.to_string(),
            line: line.to_string(),
        };

        // Store log entry
        let log_key = Self::log_key(job_id, next_seq);
        let entry_bytes = serde_json::to_vec(&entry)?;
        tx.set(COL_JOB_LOGS, log_key.as_bytes().to_vec(), entry_bytes);

        tx.commit(&self.store)?;
        Ok(entry)
    }

    pub fn get_logs(&self, job_id: &JobId, from_seq: u64, limit: usize) -> Result<Vec<LogEntry>> {
        let tx = ReadTransaction::new();
        let log_prefix = format!("{}:", job_id);
        let items = tx.scan_prefix(&self.store, COL_JOB_LOGS, log_prefix.as_bytes())?;

        let mut out = Vec::new();
        for (key_bytes, value_bytes) in items {
            let key_str = String::from_utf8_lossy(&key_bytes);

            // Extract sequence number from key (format: "job_id:seq")
            if let Some(seq_part) = key_str.split(':').nth(1) {
                if let Ok(seq) = seq_part.parse::<u64>() {
                    if seq <= from_seq {
                        continue; // Skip logs we've already seen
                    }

                    let entry: LogEntry = serde_json::from_slice(&value_bytes)?;
                    out.push(entry);

                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }

        // Sort by sequence number to ensure proper ordering
        out.sort_by(|a, b| a.seq.cmp(&b.seq));
        Ok(out)
    }

    pub fn save_artifacts(&self, job_id: &JobId, artifacts: &[JobArtifact]) -> Result<()> {
        let bytes = serde_json::to_vec(artifacts)?;
        let key = job_id.to_string();

        let mut tx = WriteTransaction::new();
        tx.set(COL_JOB_ARTIFACTS, key.as_bytes().to_vec(), bytes);
        tx.commit(&self.store)?;
        Ok(())
    }

    pub fn get_artifacts(&self, job_id: &JobId) -> Result<Vec<JobArtifact>> {
        let key = job_id.to_string();
        let tx = ReadTransaction::new();

        if let Some(bytes) = tx.get(&self.store, COL_JOB_ARTIFACTS, key.as_bytes())? {
            let artifacts: Vec<JobArtifact> = serde_json::from_slice(&bytes)?;
            Ok(artifacts)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn clear_artifacts(&self, job_id: &JobId) -> Result<()> {
        let key = job_id.to_string();

        let mut tx = WriteTransaction::new();
        tx.delete(COL_JOB_ARTIFACTS, key.as_bytes().to_vec());
        tx.commit(&self.store)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifacts_roundtrip() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let storage = Storage::new(tmp.path().join("jobs_fjall"))?;
        let job_id = JobId::new_v4();
        let artifacts = vec![JobArtifact {
            name: "result.json".to_string(),
            sha256: "abc123".to_string(),
            size: 42,
        }];
        storage.save_artifacts(&job_id, &artifacts)?;
        let loaded = storage.get_artifacts(&job_id)?;
        assert_eq!(loaded, artifacts);
        storage.clear_artifacts(&job_id)?;
        let cleared = storage.get_artifacts(&job_id)?;
        assert!(cleared.is_empty());
        Ok(())
    }
}
