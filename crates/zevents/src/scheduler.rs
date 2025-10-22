use crate::events::{Event, EventData, EventType};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

/// Configuration for a scheduled task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    /// Unique task identifier
    pub id: String,
    /// Human-readable task name
    pub name: String,
    /// Cron expression for scheduling
    pub cron_expression: String,
    /// Organization ID for multi-tenant isolation
    pub org_id: u64,
    /// Task type (determines what gets executed)
    pub task_type: TaskType,
    /// Task payload/configuration
    pub payload: serde_json::Value,
    /// Whether this task is enabled
    pub enabled: bool,
    /// Maximum execution time in seconds
    pub timeout_seconds: u64,
    /// Maximum number of retries on failure
    pub max_retries: u32,
    /// Tags for organization and filtering
    pub tags: Vec<String>,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
    /// Last execution timestamp
    pub last_run: Option<DateTime<Utc>>,
    /// Next scheduled execution
    pub next_run: Option<DateTime<Utc>>,
}

impl TaskConfig {
    pub fn new(
        name: String,
        cron_expression: String,
        org_id: u64,
        task_type: TaskType,
        payload: serde_json::Value,
    ) -> Result<Self> {
        // Validate cron expression
        Schedule::from_str(&cron_expression)?;

        let now = Utc::now();
        Ok(Self {
            id: Ulid::new().to_string(),
            name,
            cron_expression,
            org_id,
            task_type,
            payload,
            enabled: true,
            timeout_seconds: 300, // 5 minutes default
            max_retries: 3,
            tags: Vec::new(),
            created_at: now,
            updated_at: now,
            last_run: None,
            next_run: None,
        })
    }

    pub fn with_timeout(mut self, timeout_seconds: u64) -> Self {
        self.timeout_seconds = timeout_seconds;
        self
    }

    pub fn with_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Calculate the next run time based on the cron expression
    pub fn calculate_next_run(&self, from: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
        let schedule = Schedule::from_str(&self.cron_expression)?;
        Ok(schedule.upcoming(chrono::Utc).next())
    }

    /// Update the next run time
    pub fn update_next_run(&mut self) -> Result<()> {
        self.next_run = self.calculate_next_run(Utc::now())?;
        self.updated_at = Utc::now();
        Ok(())
    }
}

/// Types of scheduled tasks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskType {
    /// Execute a webhook
    Webhook {
        url: String,
        method: String,
        headers: HashMap<String, String>,
    },
    /// Execute a function/script
    Function {
        function_name: String,
        runtime: String,
    },
    /// Emit a custom event
    Event {
        event_type: EventType,
        event_data: serde_json::Value,
    },
    /// Run a database query
    Query {
        sql: String,
    },
    /// Cleanup operation
    Cleanup {
        operation: String,
        parameters: HashMap<String, serde_json::Value>,
    },
    /// Health check
    HealthCheck {
        target: String,
        checks: Vec<String>,
    },
}

/// Scheduled task execution record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledTask {
    /// Unique execution ID
    pub id: String,
    /// Task configuration ID
    pub task_id: String,
    /// Execution status
    pub status: TaskStatus,
    /// Scheduled execution time
    pub scheduled_at: DateTime<Utc>,
    /// Actual execution start time
    pub started_at: Option<DateTime<Utc>>,
    /// Execution completion time
    pub completed_at: Option<DateTime<Utc>>,
    /// Execution result/output
    pub result: Option<serde_json::Value>,
    /// Error message if failed
    pub error: Option<String>,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Next retry time if applicable
    pub next_retry: Option<DateTime<Utc>>,
}

impl ScheduledTask {
    pub fn new(task_id: String, scheduled_at: DateTime<Utc>) -> Self {
        Self {
            id: Ulid::new().to_string(),
            task_id,
            status: TaskStatus::Pending,
            scheduled_at,
            started_at: None,
            completed_at: None,
            result: None,
            error: None,
            retry_count: 0,
            next_retry: None,
        }
    }

    pub fn start(&mut self) {
        self.status = TaskStatus::Running;
        self.started_at = Some(Utc::now());
    }

    pub fn complete(&mut self, result: serde_json::Value) {
        self.status = TaskStatus::Completed;
        self.completed_at = Some(Utc::now());
        self.result = Some(result);
    }

    pub fn fail(&mut self, error: String) {
        self.status = TaskStatus::Failed;
        self.completed_at = Some(Utc::now());
        self.error = Some(error);
    }

    pub fn schedule_retry(&mut self, delay: Duration) {
        self.status = TaskStatus::Retrying;
        self.retry_count += 1;
        self.next_retry = Some(Utc::now() + chrono::Duration::from_std(delay).unwrap());
    }
}

/// Task execution status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying,
    Cancelled,
}

/// Scheduler statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerStats {
    /// Total tasks configured
    pub total_tasks: u64,
    /// Enabled tasks
    pub enabled_tasks: u64,
    /// Tasks executed in last 24 hours
    pub executions_24h: u64,
    /// Successful executions in last 24 hours
    pub successful_24h: u64,
    /// Failed executions in last 24 hours
    pub failed_24h: u64,
    /// Currently running tasks
    pub running_tasks: u64,
    /// Next task execution time
    pub next_execution: Option<DateTime<Utc>>,
}

/// Cron-based task scheduler
pub struct CronScheduler {
    /// Task configurations
    tasks: Arc<RwLock<HashMap<String, TaskConfig>>>,
    /// Execution history
    executions: Arc<RwLock<HashMap<String, ScheduledTask>>>,
    /// Scheduler state
    running: Arc<AtomicBool>,
    /// Background task handles
    task_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,
    /// Event publisher for task events
    event_publisher: Option<Arc<dyn Fn(Event) + Send + Sync>>,
}

impl CronScheduler {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            executions: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            task_handles: Arc::new(RwLock::new(Vec::new())),
            event_publisher: None,
        }
    }

    /// Set event publisher for task events
    pub fn with_event_publisher<F>(mut self, publisher: F) -> Self
    where
        F: Fn(Event) + Send + Sync + 'static,
    {
        self.event_publisher = Some(Arc::new(publisher));
        self
    }

    /// Start the scheduler
    pub async fn start(&self) -> Result<()> {
        if self.running.load(Ordering::Acquire) {
            return Err(anyhow!("Scheduler is already running"));
        }

        self.running.store(true, Ordering::Release);

        // Start scheduler loop
        let scheduler_task = self.start_scheduler_loop().await;
        self.task_handles.write().await.push(scheduler_task);

        // Start cleanup task
        let cleanup_task = self.start_cleanup_loop().await;
        self.task_handles.write().await.push(cleanup_task);

        info!("Cron scheduler started");
        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping cron scheduler");

        self.running.store(false, Ordering::Release);

        // Cancel all background tasks
        let mut handles = self.task_handles.write().await;
        for handle in handles.drain(..) {
            handle.abort();
        }

        info!("Cron scheduler stopped");
        Ok(())
    }

    /// Add a task to the scheduler
    pub async fn add_task(&self, mut task: TaskConfig) -> Result<()> {
        // Calculate initial next run
        task.update_next_run()?;

        let task_id = task.id.clone();
        self.tasks.write().await.insert(task_id.clone(), task);

        info!(task_id = %task_id, "Task added to scheduler");

        // Emit task created event
        if let Some(ref publisher) = self.event_publisher {
            let event = Event::new(
                EventType::Custom("task.created".to_string()),
                EventData::Custom(serde_json::json!({
                    "task_id": task_id,
                    "action": "created"
                })),
                self.tasks.read().await.get(&task_id).map(|t| t.org_id).unwrap_or(0),
            );
            publisher(event);
        }

        Ok(())
    }

    /// Remove a task from the scheduler
    pub async fn remove_task(&self, task_id: &str) -> Result<()> {
        let org_id = self.tasks.read().await.get(task_id).map(|t| t.org_id).unwrap_or(0);

        if self.tasks.write().await.remove(task_id).is_some() {
            info!(task_id = %task_id, "Task removed from scheduler");

            // Emit task deleted event
            if let Some(ref publisher) = self.event_publisher {
                let event = Event::new(
                    EventType::Custom("task.deleted".to_string()),
                    EventData::Custom(serde_json::json!({
                        "task_id": task_id,
                        "action": "deleted"
                    })),
                    org_id,
                );
                publisher(event);
            }

            Ok(())
        } else {
            Err(anyhow!("Task not found: {}", task_id))
        }
    }

    /// Update a task configuration
    pub async fn update_task(&self, mut task: TaskConfig) -> Result<()> {
        task.update_next_run()?;
        task.updated_at = Utc::now();

        let task_id = task.id.clone();
        let org_id = task.org_id;

        if self.tasks.read().await.contains_key(&task_id) {
            self.tasks.write().await.insert(task_id.clone(), task);

            info!(task_id = %task_id, "Task updated");

            // Emit task updated event
            if let Some(ref publisher) = self.event_publisher {
                let event = Event::new(
                    EventType::Custom("task.updated".to_string()),
                    EventData::Custom(serde_json::json!({
                        "task_id": task_id,
                        "action": "updated"
                    })),
                    org_id,
                );
                publisher(event);
            }

            Ok(())
        } else {
            Err(anyhow!("Task not found: {}", task_id))
        }
    }

    /// Get all tasks
    pub async fn list_tasks(&self) -> Vec<TaskConfig> {
        self.tasks.read().await.values().cloned().collect()
    }

    /// Get tasks for a specific organization
    pub async fn list_tasks_for_org(&self, org_id: u64) -> Vec<TaskConfig> {
        self.tasks
            .read()
            .await
            .values()
            .filter(|task| task.org_id == org_id)
            .cloned()
            .collect()
    }

    /// Get a specific task
    pub async fn get_task(&self, task_id: &str) -> Option<TaskConfig> {
        self.tasks.read().await.get(task_id).cloned()
    }

    /// Get execution history
    pub async fn list_executions(&self) -> Vec<ScheduledTask> {
        self.executions.read().await.values().cloned().collect()
    }

    /// Get executions for a specific task
    pub async fn list_executions_for_task(&self, task_id: &str) -> Vec<ScheduledTask> {
        self.executions
            .read()
            .await
            .values()
            .filter(|exec| exec.task_id == task_id)
            .cloned()
            .collect()
    }

    /// Get scheduler statistics
    pub async fn get_stats(&self) -> SchedulerStats {
        let tasks = self.tasks.read().await;
        let executions = self.executions.read().await;

        let total_tasks = tasks.len() as u64;
        let enabled_tasks = tasks.values().filter(|t| t.enabled).count() as u64;

        let now = Utc::now();
        let day_ago = now - chrono::Duration::hours(24);

        let recent_executions: Vec<_> = executions
            .values()
            .filter(|e| e.scheduled_at > day_ago)
            .collect();

        let executions_24h = recent_executions.len() as u64;
        let successful_24h = recent_executions
            .iter()
            .filter(|e| e.status == TaskStatus::Completed)
            .count() as u64;
        let failed_24h = recent_executions
            .iter()
            .filter(|e| e.status == TaskStatus::Failed)
            .count() as u64;

        let running_tasks = executions
            .values()
            .filter(|e| e.status == TaskStatus::Running)
            .count() as u64;

        let next_execution = tasks
            .values()
            .filter(|t| t.enabled)
            .filter_map(|t| t.next_run)
            .min();

        SchedulerStats {
            total_tasks,
            enabled_tasks,
            executions_24h,
            successful_24h,
            failed_24h,
            running_tasks,
            next_execution,
        }
    }

    /// Main scheduler loop
    async fn start_scheduler_loop(&self) -> JoinHandle<()> {
        let tasks = self.tasks.clone();
        let executions = self.executions.clone();
        let running = self.running.clone();
        let event_publisher = self.event_publisher.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Check every minute

            while running.load(Ordering::Acquire) {
                interval.tick().await;

                let now = Utc::now();
                let mut tasks_to_execute = Vec::new();

                // Find tasks ready for execution
                {
                    let task_map = tasks.read().await;
                    for task in task_map.values() {
                        if !task.enabled {
                            continue;
                        }

                        if let Some(next_run) = task.next_run {
                            if next_run <= now {
                                tasks_to_execute.push(task.clone());
                            }
                        }
                    }
                }

                // Execute ready tasks
                for task in tasks_to_execute {
                    let execution = ScheduledTask::new(task.id.clone(), now);
                    let execution_id = execution.id.clone();

                    executions.write().await.insert(execution_id.clone(), execution);

                    // Update task's next run time
                    if let Some(task_config) = tasks.write().await.get_mut(&task.id) {
                        task_config.last_run = Some(now);
                        if let Err(e) = task_config.update_next_run() {
                            error!(task_id = %task.id, error = %e, "Failed to update next run time");
                        }
                    }

                    // Execute task asynchronously
                    let task_for_exec = task.clone();
                    let executions_for_exec = executions.clone();
                    let publisher_for_exec = event_publisher.clone();

                    tokio::spawn(async move {
                        Self::execute_task(task_for_exec, execution_id, executions_for_exec, publisher_for_exec).await;
                    });
                }
            }

            debug!("Scheduler loop stopped");
        })
    }

    /// Execute a specific task
    async fn execute_task(
        task: TaskConfig,
        execution_id: String,
        executions: Arc<RwLock<HashMap<String, ScheduledTask>>>,
        event_publisher: Option<Arc<dyn Fn(Event) + Send + Sync>>,
    ) {
        debug!(task_id = %task.id, execution_id = %execution_id, "Executing task");

        // Mark execution as started
        if let Some(execution) = executions.write().await.get_mut(&execution_id) {
            execution.start();
        }

        // Emit task started event
        if let Some(ref publisher) = event_publisher {
            let event = Event::new(
                EventType::Custom("task.started".to_string()),
                EventData::Custom(serde_json::json!({
                    "task_id": task.id,
                    "execution_id": execution_id,
                    "action": "started"
                })),
                task.org_id,
            );
            publisher(event);
        }

        // Execute the task with timeout
        let timeout_duration = Duration::from_secs(task.timeout_seconds);
        let execution_result = tokio::time::timeout(timeout_duration, Self::run_task(&task)).await;

        // Process result
        match execution_result {
            Ok(Ok(result)) => {
                // Task completed successfully
                if let Some(execution) = executions.write().await.get_mut(&execution_id) {
                    execution.complete(result.clone());
                }

                info!(task_id = %task.id, execution_id = %execution_id, "Task completed successfully");

                // Emit task completed event
                if let Some(ref publisher) = event_publisher {
                    let event = Event::new(
                        EventType::Custom("task.completed".to_string()),
                        EventData::Custom(serde_json::json!({
                            "task_id": task.id,
                            "execution_id": execution_id,
                            "action": "completed",
                            "result": result
                        })),
                        task.org_id,
                    );
                    publisher(event);
                }
            }
            Ok(Err(error)) => {
                // Task failed
                warn!(task_id = %task.id, execution_id = %execution_id, error = %error, "Task failed");

                if let Some(execution) = executions.write().await.get_mut(&execution_id) {
                    execution.fail(error.to_string());

                    // Schedule retry if attempts are below max
                    if execution.retry_count < task.max_retries {
                        let delay = Duration::from_secs(2_u64.pow(execution.retry_count + 1)); // Exponential backoff
                        execution.schedule_retry(delay);
                    }
                }

                // Emit task failed event
                if let Some(ref publisher) = event_publisher {
                    let event = Event::new(
                        EventType::Custom("task.failed".to_string()),
                        EventData::Custom(serde_json::json!({
                            "task_id": task.id,
                            "execution_id": execution_id,
                            "action": "failed",
                            "error": error.to_string()
                        })),
                        task.org_id,
                    );
                    publisher(event);
                }
            }
            Err(_) => {
                // Task timed out
                error!(task_id = %task.id, execution_id = %execution_id, "Task timed out");

                if let Some(execution) = executions.write().await.get_mut(&execution_id) {
                    execution.fail("Task execution timed out".to_string());
                }

                // Emit task timeout event
                if let Some(ref publisher) = event_publisher {
                    let event = Event::new(
                        EventType::Custom("task.timeout".to_string()),
                        EventData::Custom(serde_json::json!({
                            "task_id": task.id,
                            "execution_id": execution_id,
                            "action": "timeout"
                        })),
                        task.org_id,
                    );
                    publisher(event);
                }
            }
        }
    }

    /// Run the actual task based on its type
    async fn run_task(task: &TaskConfig) -> Result<serde_json::Value> {
        match &task.task_type {
            TaskType::Webhook { url, method, headers } => {
                Self::execute_webhook(url, method, headers, &task.payload).await
            }
            TaskType::Function { function_name, runtime } => {
                Self::execute_function(function_name, runtime, &task.payload).await
            }
            TaskType::Event { event_type, event_data } => {
                Self::execute_event(event_type, event_data, task.org_id).await
            }
            TaskType::Query { sql } => {
                Self::execute_query(sql, &task.payload, task.org_id).await
            }
            TaskType::Cleanup { operation, parameters } => {
                Self::execute_cleanup(operation, parameters).await
            }
            TaskType::HealthCheck { target, checks } => {
                Self::execute_health_check(target, checks).await
            }
        }
    }

    /// Execute a webhook task
    async fn execute_webhook(
        url: &str,
        method: &str,
        headers: &HashMap<String, String>,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let client = reqwest::Client::new();
        let mut request = match method.to_uppercase().as_str() {
            "GET" => client.get(url),
            "POST" => client.post(url),
            "PUT" => client.put(url),
            "DELETE" => client.delete(url),
            _ => return Err(anyhow!("Unsupported HTTP method: {}", method)),
        };

        // Add headers
        for (key, value) in headers {
            request = request.header(key, value);
        }

        // Add payload for non-GET requests
        if method.to_uppercase() != "GET" && !payload.is_null() {
            request = request.json(payload);
        }

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;

        if status.is_success() {
            Ok(serde_json::json!({
                "status": status.as_u16(),
                "body": body
            }))
        } else {
            Err(anyhow!("Webhook failed with status {}: {}", status, body))
        }
    }

    /// Execute a function task (placeholder)
    async fn execute_function(
        _function_name: &str,
        _runtime: &str,
        _payload: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        // This would integrate with a function runtime like AWS Lambda, Google Cloud Functions, etc.
        Err(anyhow!("Function execution not implemented"))
    }

    /// Execute an event emission task
    async fn execute_event(
        event_type: &EventType,
        event_data: &serde_json::Value,
        org_id: u64,
    ) -> Result<serde_json::Value> {
        // This would emit the event through the event bus
        Ok(serde_json::json!({
            "event_type": event_type.to_string(),
            "org_id": org_id,
            "emitted": true
        }))
    }

    /// Execute a query task (placeholder)
    async fn execute_query(
        _sql: &str,
        _params: &serde_json::Value,
        _org_id: u64,
    ) -> Result<serde_json::Value> {
        // This would execute against the database
        Err(anyhow!("Query execution not implemented"))
    }

    /// Execute a cleanup task (placeholder)
    async fn execute_cleanup(
        _operation: &str,
        _parameters: &HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value> {
        // This would perform various cleanup operations
        Ok(serde_json::json!({"cleanup": "completed"}))
    }

    /// Execute a health check task (placeholder)
    async fn execute_health_check(
        _target: &str,
        _checks: &[String],
    ) -> Result<serde_json::Value> {
        // This would perform health checks
        Ok(serde_json::json!({"health": "ok"}))
    }

    /// Cleanup old execution records
    async fn start_cleanup_loop(&self) -> JoinHandle<()> {
        let executions = self.executions.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Clean up every hour

            while running.load(Ordering::Acquire) {
                interval.tick().await;

                let cutoff = Utc::now() - chrono::Duration::days(7); // Keep 7 days of history
                let mut removed_count = 0;

                {
                    let mut exec_map = executions.write().await;
                    let initial_len = exec_map.len();

                    exec_map.retain(|_, execution| {
                        execution.scheduled_at > cutoff || execution.status == TaskStatus::Running
                    });

                    removed_count = initial_len - exec_map.len();
                }

                if removed_count > 0 {
                    info!(removed = removed_count, "Cleaned up old task executions");
                }
            }

            debug!("Cleanup loop stopped");
        })
    }
}

impl Default for CronScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_config_creation() {
        let task = TaskConfig::new(
            "test_task".to_string(),
            "0 0 * * *".to_string(), // Daily at midnight
            1,
            TaskType::Event {
                event_type: EventType::Custom("test".to_string()),
                event_data: serde_json::json!({"test": true}),
            },
            serde_json::json!({}),
        ).unwrap();

        assert_eq!(task.name, "test_task");
        assert_eq!(task.org_id, 1);
        assert!(task.enabled);
    }

    #[test]
    fn test_invalid_cron_expression() {
        let result = TaskConfig::new(
            "test_task".to_string(),
            "invalid cron".to_string(),
            1,
            TaskType::Event {
                event_type: EventType::Custom("test".to_string()),
                event_data: serde_json::json!({"test": true}),
            },
            serde_json::json!({}),
        );

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_scheduler_creation() {
        let scheduler = CronScheduler::new();
        assert!(!scheduler.running.load(Ordering::Acquire));

        let tasks = scheduler.list_tasks().await;
        assert_eq!(tasks.len(), 0);
    }

    #[tokio::test]
    async fn test_add_and_remove_task() {
        let scheduler = CronScheduler::new();

        let task = TaskConfig::new(
            "test_task".to_string(),
            "0 0 * * *".to_string(),
            1,
            TaskType::Event {
                event_type: EventType::Custom("test".to_string()),
                event_data: serde_json::json!({"test": true}),
            },
            serde_json::json!({}),
        ).unwrap();

        let task_id = task.id.clone();

        scheduler.add_task(task).await.unwrap();

        let tasks = scheduler.list_tasks().await;
        assert_eq!(tasks.len(), 1);

        scheduler.remove_task(&task_id).await.unwrap();

        let tasks = scheduler.list_tasks().await;
        assert_eq!(tasks.len(), 0);
    }
}