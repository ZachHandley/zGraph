//! Production monitoring dashboard configurations for ZRUSTDB
//!
//! This module provides comprehensive Grafana dashboard configurations
//! for monitoring ZRUSTDB in production environments.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Grafana dashboard configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrafanaDashboard {
    pub id: Option<u64>,
    pub uid: Option<String>,
    pub title: String,
    pub tags: Vec<String>,
    pub timezone: String,
    pub panels: Vec<Panel>,
    pub templating: Templating,
    pub time: TimeRange,
    pub refresh: String,
}

/// Dashboard panel configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Panel {
    pub id: u64,
    pub title: String,
    #[serde(rename = "type")]
    pub panel_type: String,
    pub targets: Vec<QueryTarget>,
    pub grid_pos: GridPosition,
    pub options: PanelOptions,
    pub field_config: FieldConfig,
}

/// Query target for metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTarget {
    pub expr: String,
    pub legend_format: String,
    pub ref_id: String,
    pub interval: Option<String>,
}

/// Panel grid position
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridPosition {
    pub h: u64,
    pub w: u64,
    pub x: u64,
    pub y: u64,
}

/// Panel options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PanelOptions {
    pub unit: Option<String>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub decimals: Option<u64>,
    pub legend: LegendOptions,
}

/// Legend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegendOptions {
    pub display_mode: String,
    pub placement: String,
    pub values: Vec<String>,
}

/// Field configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConfig {
    pub defaults: FieldDefaults,
    pub overrides: Vec<FieldOverride>,
}

/// Default field configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefaults {
    pub unit: Option<String>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub decimals: Option<u64>,
    pub color: ColorConfig,
    pub thresholds: ThresholdConfig,
}

/// Color configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColorConfig {
    pub mode: String,
    pub fixed_color: Option<String>,
}

/// Threshold configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdConfig {
    pub mode: String,
    pub steps: Vec<ThresholdStep>,
}

/// Threshold step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdStep {
    pub color: String,
    pub value: Option<f64>,
}

/// Field override
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldOverride {
    pub matcher: FieldMatcher,
    pub properties: Vec<FieldProperty>,
}

/// Field matcher
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMatcher {
    pub id: String,
    pub options: String,
}

/// Field property
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldProperty {
    pub id: String,
    pub value: serde_json::Value,
}

/// Dashboard templating
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Templating {
    pub list: Vec<TemplateVariable>,
}

/// Template variable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateVariable {
    pub name: String,
    pub label: String,
    #[serde(rename = "type")]
    pub var_type: String,
    pub query: String,
    pub refresh: String,
    pub multi: bool,
    pub include_all: bool,
}

/// Time range configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub from: String,
    pub to: String,
}

/// Dashboard configuration manager
pub struct DashboardConfigManager;

impl DashboardConfigManager {
    /// Generate ZRUSTDB overview dashboard
    pub fn generate_overview_dashboard() -> GrafanaDashboard {
        GrafanaDashboard {
            id: None,
            uid: Some("zrustdb-overview".to_string()),
            title: "ZRUSTDB Production Overview".to_string(),
            tags: vec!["zrustdb".to_string(), "overview".to_string()],
            timezone: "browser".to_string(),
            panels: vec![
                // System Health Panel
                Panel {
                    id: 1,
                    title: "System Health Status".to_string(),
                    panel_type: "stat".to_string(),
                    targets: vec![
                        QueryTarget {
                            expr: "up{job=\"zrustdb\"}".to_string(),
                            legend_format: "{{instance}}".to_string(),
                            ref_id: "A".to_string(),
                            interval: None,
                        }
                    ],
                    grid_pos: GridPosition { h: 8, w: 12, x: 0, y: 0 },
                    options: PanelOptions {
                        unit: None,
                        min: Some(0.0),
                        max: Some(1.0),
                        decimals: Some(0),
                        legend: LegendOptions {
                            display_mode: "table".to_string(),
                            placement: "bottom".to_string(),
                            values: vec!["current".to_string()],
                        },
                    },
                    field_config: FieldConfig {
                        defaults: FieldDefaults {
                            unit: None,
                            min: Some(0.0),
                            max: Some(1.0),
                            decimals: Some(0),
                            color: ColorConfig {
                                mode: "thresholds".to_string(),
                                fixed_color: None,
                            },
                            thresholds: ThresholdConfig {
                                mode: "absolute".to_string(),
                                steps: vec![
                                    ThresholdStep { color: "red".to_string(), value: Some(0.0) },
                                    ThresholdStep { color: "green".to_string(), value: Some(1.0) },
                                ],
                            },
                        },
                        overrides: vec![],
                    },
                },

                // Request Rate Panel
                Panel {
                    id: 2,
                    title: "Request Rate".to_string(),
                    panel_type: "timeseries".to_string(),
                    targets: vec![
                        QueryTarget {
                            expr: "rate(zdb_request_duration_seconds_count[5m])".to_string(),
                            legend_format: "{{endpoint}}".to_string(),
                            ref_id: "A".to_string(),
                            interval: Some("1m".to_string()),
                        }
                    ],
                    grid_pos: GridPosition { h: 8, w: 12, x: 12, y: 0 },
                    options: PanelOptions {
                        unit: Some("reqps".to_string()),
                        min: Some(0.0),
                        max: None,
                        decimals: Some(2),
                        legend: LegendOptions {
                            display_mode: "table".to_string(),
                            placement: "bottom".to_string(),
                            values: vec!["current".to_string(), "max".to_string()],
                        },
                    },
                    field_config: FieldConfig {
                        defaults: FieldDefaults {
                            unit: Some("reqps".to_string()),
                            min: Some(0.0),
                            max: None,
                            decimals: Some(2),
                            color: ColorConfig {
                                mode: "palette-classic".to_string(),
                                fixed_color: None,
                            },
                            thresholds: ThresholdConfig {
                                mode: "absolute".to_string(),
                                steps: vec![
                                    ThresholdStep { color: "green".to_string(), value: Some(0.0) },
                                    ThresholdStep { color: "yellow".to_string(), value: Some(100.0) },
                                    ThresholdStep { color: "red".to_string(), value: Some(500.0) },
                                ],
                            },
                        },
                        overrides: vec![],
                    },
                },

                // Response Time Panel
                Panel {
                    id: 3,
                    title: "Response Time Percentiles".to_string(),
                    panel_type: "timeseries".to_string(),
                    targets: vec![
                        QueryTarget {
                            expr: "histogram_quantile(0.50, rate(zdb_request_duration_seconds_bucket[5m]))".to_string(),
                            legend_format: "50th percentile".to_string(),
                            ref_id: "A".to_string(),
                            interval: Some("1m".to_string()),
                        },
                        QueryTarget {
                            expr: "histogram_quantile(0.95, rate(zdb_request_duration_seconds_bucket[5m]))".to_string(),
                            legend_format: "95th percentile".to_string(),
                            ref_id: "B".to_string(),
                            interval: Some("1m".to_string()),
                        },
                        QueryTarget {
                            expr: "histogram_quantile(0.99, rate(zdb_request_duration_seconds_bucket[5m]))".to_string(),
                            legend_format: "99th percentile".to_string(),
                            ref_id: "C".to_string(),
                            interval: Some("1m".to_string()),
                        },
                    ],
                    grid_pos: GridPosition { h: 8, w: 24, x: 0, y: 8 },
                    options: PanelOptions {
                        unit: Some("s".to_string()),
                        min: Some(0.0),
                        max: None,
                        decimals: Some(3),
                        legend: LegendOptions {
                            display_mode: "table".to_string(),
                            placement: "right".to_string(),
                            values: vec!["current".to_string(), "max".to_string()],
                        },
                    },
                    field_config: FieldConfig {
                        defaults: FieldDefaults {
                            unit: Some("s".to_string()),
                            min: Some(0.0),
                            max: None,
                            decimals: Some(3),
                            color: ColorConfig {
                                mode: "palette-classic".to_string(),
                                fixed_color: None,
                            },
                            thresholds: ThresholdConfig {
                                mode: "absolute".to_string(),
                                steps: vec![
                                    ThresholdStep { color: "green".to_string(), value: Some(0.0) },
                                    ThresholdStep { color: "yellow".to_string(), value: Some(0.1) },
                                    ThresholdStep { color: "red".to_string(), value: Some(1.0) },
                                ],
                            },
                        },
                        overrides: vec![],
                    },
                },
            ],
            templating: Templating {
                list: vec![
                    TemplateVariable {
                        name: "instance".to_string(),
                        label: "Instance".to_string(),
                        var_type: "query".to_string(),
                        query: "label_values(up{job=\"zrustdb\"}, instance)".to_string(),
                        refresh: "on_time_range_changed".to_string(),
                        multi: true,
                        include_all: true,
                    }
                ],
            },
            time: TimeRange {
                from: "now-1h".to_string(),
                to: "now".to_string(),
            },
            refresh: "30s".to_string(),
        }
    }

    /// Generate vector operations dashboard
    pub fn generate_vector_dashboard() -> GrafanaDashboard {
        GrafanaDashboard {
            id: None,
            uid: Some("zrustdb-vectors".to_string()),
            title: "ZRUSTDB Vector Operations".to_string(),
            tags: vec!["zrustdb".to_string(), "vectors".to_string()],
            timezone: "browser".to_string(),
            panels: vec![
                // Vector Operation Rate
                Panel {
                    id: 1,
                    title: "Vector Operations Rate".to_string(),
                    panel_type: "timeseries".to_string(),
                    targets: vec![
                        QueryTarget {
                            expr: "rate(zdb_vector_operations_total[5m])".to_string(),
                            legend_format: "{{operation}} - {{metric_type}}".to_string(),
                            ref_id: "A".to_string(),
                            interval: Some("1m".to_string()),
                        }
                    ],
                    grid_pos: GridPosition { h: 8, w: 12, x: 0, y: 0 },
                    options: PanelOptions {
                        unit: Some("ops".to_string()),
                        min: Some(0.0),
                        max: None,
                        decimals: Some(2),
                        legend: LegendOptions {
                            display_mode: "table".to_string(),
                            placement: "bottom".to_string(),
                            values: vec!["current".to_string()],
                        },
                    },
                    field_config: FieldConfig {
                        defaults: FieldDefaults {
                            unit: Some("ops".to_string()),
                            min: Some(0.0),
                            max: None,
                            decimals: Some(2),
                            color: ColorConfig {
                                mode: "palette-classic".to_string(),
                                fixed_color: None,
                            },
                            thresholds: ThresholdConfig {
                                mode: "absolute".to_string(),
                                steps: vec![
                                    ThresholdStep { color: "green".to_string(), value: Some(0.0) },
                                ],
                            },
                        },
                        overrides: vec![],
                    },
                },

                // Vector Operation Latency
                Panel {
                    id: 2,
                    title: "Vector Operation Latency".to_string(),
                    panel_type: "timeseries".to_string(),
                    targets: vec![
                        QueryTarget {
                            expr: "histogram_quantile(0.95, rate(zdb_vector_operation_duration_seconds_bucket[5m]))".to_string(),
                            legend_format: "95th percentile - {{operation}}".to_string(),
                            ref_id: "A".to_string(),
                            interval: Some("1m".to_string()),
                        }
                    ],
                    grid_pos: GridPosition { h: 8, w: 12, x: 12, y: 0 },
                    options: PanelOptions {
                        unit: Some("s".to_string()),
                        min: Some(0.0),
                        max: None,
                        decimals: Some(6),
                        legend: LegendOptions {
                            display_mode: "table".to_string(),
                            placement: "bottom".to_string(),
                            values: vec!["current".to_string()],
                        },
                    },
                    field_config: FieldConfig {
                        defaults: FieldDefaults {
                            unit: Some("s".to_string()),
                            min: Some(0.0),
                            max: None,
                            decimals: Some(6),
                            color: ColorConfig {
                                mode: "palette-classic".to_string(),
                                fixed_color: None,
                            },
                            thresholds: ThresholdConfig {
                                mode: "absolute".to_string(),
                                steps: vec![
                                    ThresholdStep { color: "green".to_string(), value: Some(0.0) },
                                    ThresholdStep { color: "yellow".to_string(), value: Some(0.001) },
                                    ThresholdStep { color: "red".to_string(), value: Some(0.01) },
                                ],
                            },
                        },
                        overrides: vec![],
                    },
                },

                // SIMD Usage
                Panel {
                    id: 3,
                    title: "SIMD Instruction Usage".to_string(),
                    panel_type: "piechart".to_string(),
                    targets: vec![
                        QueryTarget {
                            expr: "sum by (simd_enabled) (rate(zdb_vector_operations_total[5m]))".to_string(),
                            legend_format: "{{simd_enabled}}".to_string(),
                            ref_id: "A".to_string(),
                            interval: Some("1m".to_string()),
                        }
                    ],
                    grid_pos: GridPosition { h: 8, w: 12, x: 0, y: 8 },
                    options: PanelOptions {
                        unit: Some("ops".to_string()),
                        min: None,
                        max: None,
                        decimals: Some(0),
                        legend: LegendOptions {
                            display_mode: "table".to_string(),
                            placement: "right".to_string(),
                            values: vec!["value".to_string(), "percent".to_string()],
                        },
                    },
                    field_config: FieldConfig {
                        defaults: FieldDefaults {
                            unit: Some("ops".to_string()),
                            min: None,
                            max: None,
                            decimals: Some(0),
                            color: ColorConfig {
                                mode: "palette-classic".to_string(),
                                fixed_color: None,
                            },
                            thresholds: ThresholdConfig {
                                mode: "absolute".to_string(),
                                steps: vec![
                                    ThresholdStep { color: "green".to_string(), value: Some(0.0) },
                                ],
                            },
                        },
                        overrides: vec![],
                    },
                },
            ],
            templating: Templating {
                list: vec![
                    TemplateVariable {
                        name: "metric_type".to_string(),
                        label: "Metric Type".to_string(),
                        var_type: "query".to_string(),
                        query: "label_values(zdb_vector_operations_total, metric_type)".to_string(),
                        refresh: "on_time_range_changed".to_string(),
                        multi: true,
                        include_all: true,
                    }
                ],
            },
            time: TimeRange {
                from: "now-1h".to_string(),
                to: "now".to_string(),
            },
            refresh: "30s".to_string(),
        }
    }

    /// Generate system resources dashboard
    pub fn generate_system_dashboard() -> GrafanaDashboard {
        GrafanaDashboard {
            id: None,
            uid: Some("zrustdb-system".to_string()),
            title: "ZRUSTDB System Resources".to_string(),
            tags: vec!["zrustdb".to_string(), "system".to_string()],
            timezone: "browser".to_string(),
            panels: vec![
                // CPU Usage
                Panel {
                    id: 1,
                    title: "CPU Usage".to_string(),
                    panel_type: "timeseries".to_string(),
                    targets: vec![
                        QueryTarget {
                            expr: "zdb_cpu_usage_percent".to_string(),
                            legend_format: "CPU Usage %".to_string(),
                            ref_id: "A".to_string(),
                            interval: Some("1m".to_string()),
                        }
                    ],
                    grid_pos: GridPosition { h: 8, w: 12, x: 0, y: 0 },
                    options: PanelOptions {
                        unit: Some("percent".to_string()),
                        min: Some(0.0),
                        max: Some(100.0),
                        decimals: Some(1),
                        legend: LegendOptions {
                            display_mode: "table".to_string(),
                            placement: "bottom".to_string(),
                            values: vec!["current".to_string(), "max".to_string()],
                        },
                    },
                    field_config: FieldConfig {
                        defaults: FieldDefaults {
                            unit: Some("percent".to_string()),
                            min: Some(0.0),
                            max: Some(100.0),
                            decimals: Some(1),
                            color: ColorConfig {
                                mode: "thresholds".to_string(),
                                fixed_color: None,
                            },
                            thresholds: ThresholdConfig {
                                mode: "absolute".to_string(),
                                steps: vec![
                                    ThresholdStep { color: "green".to_string(), value: Some(0.0) },
                                    ThresholdStep { color: "yellow".to_string(), value: Some(70.0) },
                                    ThresholdStep { color: "red".to_string(), value: Some(90.0) },
                                ],
                            },
                        },
                        overrides: vec![],
                    },
                },

                // Memory Usage
                Panel {
                    id: 2,
                    title: "Memory Usage".to_string(),
                    panel_type: "timeseries".to_string(),
                    targets: vec![
                        QueryTarget {
                            expr: "zdb_memory_usage_bytes / 1024 / 1024 / 1024".to_string(),
                            legend_format: "Memory Usage (GB)".to_string(),
                            ref_id: "A".to_string(),
                            interval: Some("1m".to_string()),
                        }
                    ],
                    grid_pos: GridPosition { h: 8, w: 12, x: 12, y: 0 },
                    options: PanelOptions {
                        unit: Some("bytes".to_string()),
                        min: Some(0.0),
                        max: None,
                        decimals: Some(2),
                        legend: LegendOptions {
                            display_mode: "table".to_string(),
                            placement: "bottom".to_string(),
                            values: vec!["current".to_string(), "max".to_string()],
                        },
                    },
                    field_config: FieldConfig {
                        defaults: FieldDefaults {
                            unit: Some("bytes".to_string()),
                            min: Some(0.0),
                            max: None,
                            decimals: Some(2),
                            color: ColorConfig {
                                mode: "thresholds".to_string(),
                                fixed_color: None,
                            },
                            thresholds: ThresholdConfig {
                                mode: "absolute".to_string(),
                                steps: vec![
                                    ThresholdStep { color: "green".to_string(), value: Some(0.0) },
                                    ThresholdStep { color: "yellow".to_string(), value: Some(6.0) },
                                    ThresholdStep { color: "red".to_string(), value: Some(8.0) },
                                ],
                            },
                        },
                        overrides: vec![],
                    },
                },
            ],
            templating: Templating { list: vec![] },
            time: TimeRange {
                from: "now-1h".to_string(),
                to: "now".to_string(),
            },
            refresh: "30s".to_string(),
        }
    }

    /// Export dashboard as JSON
    pub fn export_dashboard_json(dashboard: &GrafanaDashboard) -> serde_json::Result<String> {
        serde_json::to_string_pretty(dashboard)
    }

    /// Generate all production dashboards
    pub fn generate_all_dashboards() -> Vec<GrafanaDashboard> {
        vec![
            Self::generate_overview_dashboard(),
            Self::generate_vector_dashboard(),
            Self::generate_system_dashboard(),
        ]
    }
}

/// Prometheus alerting rules configuration
pub struct PrometheusAlertRules;

impl PrometheusAlertRules {
    /// Generate Prometheus alerting rules YAML
    pub fn generate_alert_rules() -> String {
        r#"
groups:
  - name: zrustdb.rules
    rules:
      - alert: ZRustDBHighCPU
        expr: zdb_cpu_usage_percent > 80
        for: 5m
        labels:
          severity: warning
          service: zrustdb
        annotations:
          summary: "ZRUSTDB CPU usage is high"
          description: "CPU usage has been above 80% for more than 5 minutes"

      - alert: ZRustDBHighMemory
        expr: zdb_memory_usage_bytes > 8e9
        for: 3m
        labels:
          severity: warning
          service: zrustdb
        annotations:
          summary: "ZRUSTDB memory usage is high"
          description: "Memory usage has exceeded 8GB for more than 3 minutes"

      - alert: ZRustDBHighVectorLatency
        expr: histogram_quantile(0.95, rate(zdb_vector_operation_duration_seconds_bucket[5m])) > 0.1
        for: 2m
        labels:
          severity: warning
          service: zrustdb
        annotations:
          summary: "ZRUSTDB vector operations are slow"
          description: "95th percentile vector operation latency is above 100ms"

      - alert: ZRustDBAuthFailures
        expr: rate(zdb_auth_requests_total{status="failed"}[5m]) / rate(zdb_auth_requests_total[5m]) > 0.2
        for: 1m
        labels:
          severity: critical
          service: zrustdb
        annotations:
          summary: "ZRUSTDB authentication failure rate is high"
          description: "More than 20% of authentication requests are failing"

      - alert: ZRustDBJobQueueBacklog
        expr: zdb_job_queue_size > 100
        for: 5m
        labels:
          severity: warning
          service: zrustdb
        annotations:
          summary: "ZRUSTDB job queue has high backlog"
          description: "Job queue has more than 100 pending jobs for over 5 minutes"

      - alert: ZRustDBServiceDown
        expr: up{job="zrustdb"} == 0
        for: 30s
        labels:
          severity: critical
          service: zrustdb
        annotations:
          summary: "ZRUSTDB service is down"
          description: "ZRUSTDB service instance {{$labels.instance}} is down"

      - alert: ZRustDBDatabaseErrors
        expr: rate(zdb_storage_operations_total{status="error"}[5m]) > 1
        for: 2m
        labels:
          severity: critical
          service: zrustdb
        annotations:
          summary: "ZRUSTDB database errors detected"
          description: "Database operations are failing at {{$value}} errors per second"
"#.to_string()
    }
}