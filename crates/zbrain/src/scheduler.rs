use crate::{queue::JobQueue, worker::Worker};
use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

pub struct FifoScheduler {
    queue: Arc<JobQueue>,
    worker: Arc<dyn Worker>,
    org_id: String,
    poll: Duration,
}

impl FifoScheduler {
    pub fn new(queue: Arc<JobQueue>, worker: Arc<dyn Worker>, org_id: String) -> Self {
        Self {
            queue,
            worker,
            org_id,
            poll: Duration::from_secs(5),
        }
    }
    pub fn with_poll_interval(mut self, d: Duration) -> Self {
        self.poll = d;
        self
    }
    pub async fn run(&self) -> Result<()> {
        info!(
            "scheduler started for org {} worker {}",
            self.org_id,
            self.worker.id()
        );
        let mut tick = interval(self.poll);
        loop {
            tick.tick().await;
            if let Err(e) = self.worker.health_check().await {
                error!("worker health: {}", e);
                continue;
            }
            match self
                .queue
                .acquire_lease(self.worker.id(), &self.org_id)
                .await?
            {
                Some(job) => {
                    let cancel = CancellationToken::new();
                    self.queue
                        .register_cancel_token(job.id, cancel.clone())
                        .await;
                    let q = self.queue.clone();
                    let w = self.worker.clone();
                    let wid = self.worker.id().to_string();
                    let id = job.id;
                    tokio::spawn(async move {
                        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                        let exec = {
                            let w = w.clone();
                            let j = job.clone();
                            let cancel = cancel.clone();
                            tokio::spawn(async move { w.execute(j, tx, cancel).await })
                        };
                        let evh = {
                            let q = q.clone();
                            tokio::spawn(async move {
                                while let Some(ev) = rx.recv().await {
                                    let _ = q.handle_worker_event(ev).await;
                                }
                            })
                        };
                        let _ = exec.await;
                        let _ = q.release_lease(&id, &wid).await;
                        q.clear_cancel_token(&id).await;
                        evh.abort();
                        debug!("job {} done", id);
                    });
                }
                None => {
                    debug!("no jobs for org {}", self.org_id);
                }
            }
        }
    }
}
