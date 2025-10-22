//! ZRUSTDB Observability Framework
//!
//! This crate provides comprehensive monitoring, metrics, tracing, and health check
//! capabilities for ZRUSTDB production deployments.

pub mod health;
pub mod metrics;
pub mod tracing_setup;
pub mod alerts;
pub mod collectors;
pub mod service_health_checks;
pub mod distributed_tracing;
pub mod dashboard_configs;
pub mod operational_intelligence;

use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;

/// Central observability manager
#[derive(Clone)]
pub struct ObservabilityManager {
    metrics_registry: Arc<prometheus::Registry>,
    health_manager: Arc<health::HealthManager>,
    alert_manager: Option<Arc<alerts::AlertManager>>,
    tracing_manager: Option<Arc<distributed_tracing::TracingManager>>,
    operational_intelligence: Option<Arc<operational_intelligence::OperationalIntelligenceManager>>,
    config: ObservabilityConfig,
}

/// Configuration for observability stack
#[derive(Clone, Debug)]
pub struct ObservabilityConfig {
    /// Enable Prometheus metrics endpoint
    pub metrics_enabled: bool,
    /// Metrics endpoint bind address
    pub metrics_addr: SocketAddr,
    /// Enable health check endpoint
    pub health_enabled: bool,
    /// Health check endpoint bind address
    pub health_addr: SocketAddr,
    /// Enable structured logging
    pub structured_logging: bool,
    /// Service name for tracing
    pub service_name: String,
    /// Environment (production, staging, development)
    pub environment: String,
    /// Instance ID for this service instance
    pub instance_id: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            metrics_addr: "127.0.0.1:9090".parse().unwrap(),
            health_enabled: true,
            health_addr: "127.0.0.1:8080".parse().unwrap(),
            structured_logging: true,
            service_name: "zrustdb".to_string(),
            environment: "development".to_string(),
            instance_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

impl ObservabilityManager {
    /// Create new observability manager with config
    pub fn new(config: ObservabilityConfig) -> Result<Self> {
        let metrics_registry = Arc::new(prometheus::Registry::new());
        let health_manager = Arc::new(health::HealthManager::new());

        Ok(Self {
            metrics_registry,
            health_manager,
            alert_manager: None,
            tracing_manager: None,
            operational_intelligence: None,
            config,
        })
    }

    /// Enable alerting system
    pub fn with_alerting(mut self) -> Result<(Self, tokio::sync::mpsc::UnboundedReceiver<alerts::AlertEvent>)> {
        let (alert_manager, alert_receiver) = alerts::AlertManager::new();
        self.alert_manager = Some(Arc::new(alert_manager));
        Ok((self, alert_receiver))
    }

    /// Enable distributed tracing
    pub fn with_tracing(mut self) -> Self {
        let tracing_manager = distributed_tracing::TracingManager::new(self.config.service_name.clone());
        self.tracing_manager = Some(Arc::new(tracing_manager));
        self
    }

    /// Enable operational intelligence
    pub fn with_operational_intelligence(mut self) -> Self {
        let operational_intelligence = operational_intelligence::OperationalIntelligenceManager::new();
        self.operational_intelligence = Some(Arc::new(operational_intelligence));
        self
    }

    /// Initialize all observability components
    pub async fn initialize(&self) -> Result<()> {
        // Initialize metrics collection
        if self.config.metrics_enabled {
            self.init_metrics().await?;
        }

        // Initialize structured logging
        if self.config.structured_logging {
            self.init_structured_logging().await?;
        }

        // Start health check system
        if self.config.health_enabled {
            Arc::clone(&self.health_manager).start().await?;
        }

        tracing::info!("Observability stack initialized successfully");
        Ok(())
    }

    /// Initialize metrics collection
    async fn init_metrics(&self) -> Result<()> {
        // Register process collector for system metrics
        #[cfg(feature = "process")]
        {
            use prometheus::process_collector::ProcessCollector;
            let process_collector = ProcessCollector::for_self();
            self.metrics_registry.register(Box::new(process_collector))?;
        }

        tracing::info!("Metrics collection initialized");
        Ok(())
    }

    /// Initialize structured logging
    async fn init_structured_logging(&self) -> Result<()> {
        tracing_setup::init_structured_logging(&self.config).await?;
        Ok(())
    }

    /// Get metrics registry for custom metrics
    pub fn metrics_registry(&self) -> Arc<prometheus::Registry> {
        self.metrics_registry.clone()
    }

    /// Get health manager
    pub fn health_manager(&self) -> Arc<health::HealthManager> {
        self.health_manager.clone()
    }

    /// Get alert manager
    pub fn alert_manager(&self) -> Option<Arc<alerts::AlertManager>> {
        self.alert_manager.clone()
    }

    /// Get tracing manager
    pub fn tracing_manager(&self) -> Option<Arc<distributed_tracing::TracingManager>> {
        self.tracing_manager.clone()
    }

    /// Get operational intelligence manager
    pub fn operational_intelligence(&self) -> Option<Arc<operational_intelligence::OperationalIntelligenceManager>> {
        self.operational_intelligence.clone()
    }

    /// Start observability HTTP servers
    pub async fn start_servers(&self) -> Result<()> {
        // Start metrics server
        if self.config.metrics_enabled {
            let registry = self.metrics_registry.clone();
            let addr = self.config.metrics_addr;

            tokio::spawn(async move {
                if let Err(e) = metrics::start_metrics_server(registry, addr).await {
                    tracing::error!("Metrics server error: {}", e);
                }
            });

            tracing::info!("Metrics server started on {}", self.config.metrics_addr);
        }

        // Start health check server
        if self.config.health_enabled {
            let health_manager = self.health_manager.clone();
            let addr = self.config.health_addr;

            tokio::spawn(async move {
                if let Err(e) = health::start_health_server(health_manager, addr).await {
                    tracing::error!("Health check server error: {}", e);
                }
            });

            tracing::info!("Health check server started on {}", self.config.health_addr);
        }

        Ok(())
    }

    /// Shutdown observability components gracefully
    pub async fn shutdown(&self) -> Result<()> {
        self.health_manager.shutdown().await?;
        tracing::info!("Observability stack shutdown complete");
        Ok(())
    }
}

/// Create observability manager from environment
pub fn from_env() -> Result<ObservabilityManager> {
    let config = ObservabilityConfig {
        metrics_enabled: std::env::var("ZOBS_METRICS_ENABLED")
            .map(|v| v.parse().unwrap_or(true))
            .unwrap_or(true),
        metrics_addr: std::env::var("ZOBS_METRICS_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:9090".to_string())
            .parse()?,
        health_enabled: std::env::var("ZOBS_HEALTH_ENABLED")
            .map(|v| v.parse().unwrap_or(true))
            .unwrap_or(true),
        health_addr: std::env::var("ZOBS_HEALTH_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse()?,
        structured_logging: std::env::var("ZOBS_STRUCTURED_LOGGING")
            .map(|v| v.parse().unwrap_or(true))
            .unwrap_or(true),
        service_name: std::env::var("ZOBS_SERVICE_NAME")
            .unwrap_or_else(|_| "zrustdb".to_string()),
        environment: std::env::var("ZOBS_ENVIRONMENT")
            .unwrap_or_else(|_| "development".to_string()),
        instance_id: std::env::var("ZOBS_INSTANCE_ID")
            .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string()),
    };

    ObservabilityManager::new(config)
}

// Re-export important types
pub use health::{HealthManager, HealthCheck, HealthStatus};
pub use metrics::{ZdbMetrics, MetricsCollector};
pub use alerts::{AlertManager, AlertEvent, AlertNotificationService};
pub use distributed_tracing::{TracingManager, Tracer, SpanContext};
pub use operational_intelligence::{OperationalIntelligenceManager, OperationalInsights};
pub use dashboard_configs::DashboardConfigManager;