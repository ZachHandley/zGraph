use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

pub type JobId = Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Retrying,
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    pub org_id: String,
    pub cmd: Vec<String>,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub timeout_secs: Option<u64>,
    #[serde(default = "JobSpec::default_max_retries")]
    pub max_retries: u32,
}

impl JobSpec {
    fn default_max_retries() -> u32 {
        1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub spec: JobSpec,
    pub state: JobState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub retry_count: u32,
    pub error_message: Option<String>,
    pub worker_id: Option<String>,
    pub output: Option<serde_json::Value>,
}

impl Job {
    pub fn new(spec: JobSpec) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            spec,
            state: JobState::Pending,
            created_at: now,
            updated_at: now,
            started_at: None,
            completed_at: None,
            retry_count: 0,
            error_message: None,
            worker_id: None,
            output: None,
        }
    }

    pub fn start(&mut self, worker_id: String) {
        self.state = JobState::Running;
        self.worker_id = Some(worker_id);
        self.started_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    pub fn succeed(&mut self, output: Option<serde_json::Value>) {
        self.state = JobState::Succeeded;
        self.output = output;
        self.completed_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    pub fn fail(&mut self, error: String) {
        if self.retry_count < self.spec.max_retries {
            self.state = JobState::Retrying;
            self.retry_count += 1;
        } else {
            self.state = JobState::Failed;
            self.completed_at = Some(Utc::now());
        }
        self.error_message = Some(error);
        self.worker_id = None;
        self.updated_at = Utc::now();
    }

    pub fn cancel(&mut self) {
        self.state = JobState::Canceled;
        self.completed_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    pub fn reset_for_retry(&mut self) {
        self.state = JobState::Pending;
        self.worker_id = None;
        self.started_at = None;
        self.updated_at = Utc::now();
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            JobState::Succeeded | JobState::Failed | JobState::Canceled
        )
    }

    pub fn is_runnable(&self) -> bool {
        matches!(self.state, JobState::Pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> JobSpec {
        JobSpec {
            org_id: "test".into(),
            cmd: vec!["echo".into(), "hi".into()],
            input: json!({"k":"v"}),
            env: Default::default(),
            timeout_secs: Some(5),
            max_retries: 2,
        }
    }

    #[test]
    fn transitions() {
        let mut j = Job::new(spec());
        assert!(j.is_runnable());
        j.start("w".into());
        assert_eq!(j.state, JobState::Running);
        j.fail("x".into());
        assert_eq!(j.state, JobState::Retrying);
        j.reset_for_retry();
        assert!(j.is_runnable());
        j.start("w2".into());
        j.succeed(None);
        assert!(j.is_terminal());
    }
}
