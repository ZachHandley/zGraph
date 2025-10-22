use anyhow::Result;
use chrono::{Datelike, Timelike};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use zevents::{Event, EventData, EventHandler, EventType, HandlerResult};

/// Cache key for query results and computed data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKey {
    /// Organization ID for isolation
    pub org_id: u64,
    /// Cache category (query, aggregation, index, etc.)
    pub category: CacheCategory,
    /// Key components for building the cache key
    pub components: Vec<String>,
    /// Optional metadata for the cache entry
    pub metadata: HashMap<String, String>,
}

/// Categories of cached data
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CacheCategory {
    /// SQL query results
    Query,
    /// Vector search results
    VectorSearch,
    /// Aggregation results
    Aggregation,
    /// Table metadata
    TableMetadata,
    /// Index statistics
    IndexStats,
    /// Collection projections
    CollectionProjection,
    /// Custom cached data
    Custom(String),
}

/// Cache invalidation rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationRule {
    /// Unique rule identifier
    pub rule_id: String,
    /// Organization ID this rule applies to
    pub org_id: u64,
    /// Event types that trigger this rule
    pub trigger_events: HashSet<EventType>,
    /// Tables that trigger invalidation when changed
    pub trigger_tables: HashSet<String>,
    /// Operations that trigger invalidation
    pub trigger_operations: HashSet<String>,
    /// Cache categories to invalidate
    pub target_categories: HashSet<CacheCategory>,
    /// Patterns for cache keys to invalidate
    pub key_patterns: Vec<String>,
    /// Conditions for rule activation
    pub conditions: InvalidationConditions,
    /// Whether this rule is active
    pub is_active: bool,
    /// Rule priority (higher numbers = higher priority)
    pub priority: u32,
    /// When this rule was created
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Conditions for cache invalidation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationConditions {
    /// Minimum number of rows affected to trigger
    pub min_rows_affected: Option<u64>,
    /// Specific columns that must be affected
    pub affected_columns: Vec<String>,
    /// Time-based conditions
    pub time_conditions: Vec<TimeCondition>,
    /// Custom conditions
    pub custom_conditions: Vec<String>,
}

/// Time-based condition for invalidation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeCondition {
    /// Type of time condition
    pub condition_type: TimeConditionType,
    /// Time value or pattern
    pub value: String,
}

/// Types of time conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeConditionType {
    /// Only during certain hours
    DuringHours,
    /// After a specific time
    After,
    /// Before a specific time
    Before,
    /// On specific days of the week
    OnDays,
}

/// Cache invalidation event
#[derive(Debug, Clone)]
pub struct InvalidationEvent {
    /// Event that triggered the invalidation
    pub trigger_event: Event,
    /// Cache keys to invalidate
    pub cache_keys: Vec<CacheKey>,
    /// Invalidation reason
    pub reason: String,
    /// When the invalidation occurred
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Statistics for cache invalidation
#[derive(Debug, Clone, Default)]
pub struct InvalidationStats {
    /// Total invalidations performed
    pub total_invalidations: u64,
    /// Invalidations by category
    pub invalidations_by_category: HashMap<String, u64>,
    /// Invalidations by event type
    pub invalidations_by_event: HashMap<String, u64>,
    /// Average invalidation time in milliseconds
    pub avg_invalidation_time_ms: f64,
    /// Number of active rules
    pub active_rules: u64,
    /// Cache hit rate after invalidations
    pub cache_hit_rate: f64,
}

/// Event-driven cache invalidation manager
pub struct CacheInvalidationManager {
    /// Invalidation rules by ID
    rules: Arc<RwLock<HashMap<String, InvalidationRule>>>,
    /// Rules indexed by event type for efficient lookup
    event_index: Arc<RwLock<HashMap<EventType, Vec<String>>>>,
    /// Rules indexed by table name
    table_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Cache store abstraction
    cache_store: Arc<dyn CacheStore>,
    /// Invalidation statistics
    stats: Arc<RwLock<InvalidationStats>>,
}

/// Abstraction for different cache storage backends
#[async_trait::async_trait]
pub trait CacheStore: Send + Sync {
    /// Remove a cache entry by key
    async fn invalidate(&self, key: &CacheKey) -> Result<bool>;

    /// Remove multiple cache entries
    async fn invalidate_batch(&self, keys: &[CacheKey]) -> Result<u64>;

    /// Remove cache entries matching a pattern
    async fn invalidate_pattern(&self, pattern: &str, org_id: u64) -> Result<u64>;

    /// Get cache statistics
    async fn get_stats(&self) -> Result<CacheStats>;

    /// Check if a cache entry exists
    async fn exists(&self, key: &CacheKey) -> Result<bool>;
}

/// Cache statistics from the storage backend
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_entries: u64,
    pub total_size_bytes: u64,
    pub hit_count: u64,
    pub miss_count: u64,
    pub eviction_count: u64,
}

impl CacheKey {
    /// Create a new cache key
    pub fn new(org_id: u64, category: CacheCategory, components: Vec<String>) -> Self {
        Self {
            org_id,
            category,
            components,
            metadata: HashMap::new(),
        }
    }

    /// Create a query cache key
    pub fn query(org_id: u64, sql: &str, params: Vec<String>) -> Self {
        let mut components = vec![sql.to_string()];
        components.extend(params);
        Self::new(org_id, CacheCategory::Query, components)
    }

    /// Create a vector search cache key
    pub fn vector_search(org_id: u64, table: &str, vector: &[f32], limit: usize) -> Self {
        let vector_str = vector.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(",");
        Self::new(
            org_id,
            CacheCategory::VectorSearch,
            vec![table.to_string(), vector_str, limit.to_string()],
        )
    }

    /// Create an aggregation cache key
    pub fn aggregation(org_id: u64, table: &str, agg_func: &str, columns: Vec<String>) -> Self {
        let mut components = vec![table.to_string(), agg_func.to_string()];
        components.extend(columns);
        Self::new(org_id, CacheCategory::Aggregation, components)
    }

    /// Generate a string representation of this key
    pub fn to_string(&self) -> String {
        let category_str = match &self.category {
            CacheCategory::Query => "query",
            CacheCategory::VectorSearch => "vector_search",
            CacheCategory::Aggregation => "aggregation",
            CacheCategory::TableMetadata => "table_metadata",
            CacheCategory::IndexStats => "index_stats",
            CacheCategory::CollectionProjection => "collection_projection",
            CacheCategory::Custom(name) => name,
        };

        format!("{}:{}:{}", self.org_id, category_str, self.components.join(":"))
    }

    /// Check if this key matches a pattern
    pub fn matches_pattern(&self, pattern: &str) -> bool {
        let key_str = self.to_string();

        if pattern == "*" {
            return true;
        }

        if pattern.contains('*') {
            let regex_pattern = pattern.replace('*', ".*");
            if let Ok(regex) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
                return regex.is_match(&key_str);
            }
        }

        key_str == pattern
    }
}

impl Default for InvalidationConditions {
    fn default() -> Self {
        Self {
            min_rows_affected: None,
            affected_columns: Vec::new(),
            time_conditions: Vec::new(),
            custom_conditions: Vec::new(),
        }
    }
}

impl CacheInvalidationManager {
    /// Create a new cache invalidation manager
    pub fn new(cache_store: Arc<dyn CacheStore>) -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
            event_index: Arc::new(RwLock::new(HashMap::new())),
            table_index: Arc::new(RwLock::new(HashMap::new())),
            cache_store,
            stats: Arc::new(RwLock::new(InvalidationStats::default())),
        }
    }

    /// Add an invalidation rule
    pub async fn add_rule(&self, rule: InvalidationRule) -> Result<()> {
        let rule_id = rule.rule_id.clone();

        // Update event index
        {
            let mut event_index = self.event_index.write().await;
            for event_type in &rule.trigger_events {
                event_index
                    .entry(event_type.clone())
                    .or_insert_with(Vec::new)
                    .push(rule_id.clone());
            }
        }

        // Update table index
        {
            let mut table_index = self.table_index.write().await;
            for table in &rule.trigger_tables {
                table_index
                    .entry(table.clone())
                    .or_insert_with(Vec::new)
                    .push(rule_id.clone());
            }
        }

        // Store rule
        self.rules.write().await.insert(rule_id.clone(), rule);

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.active_rules += 1;
        }

        info!(rule_id = %rule_id, "Cache invalidation rule added");

        Ok(())
    }

    /// Remove an invalidation rule
    pub async fn remove_rule(&self, rule_id: &str) -> Result<()> {
        let rules = self.rules.read().await;

        if let Some(rule) = rules.get(rule_id) {
            // Remove from event index
            {
                let mut event_index = self.event_index.write().await;
                for event_type in &rule.trigger_events {
                    if let Some(rule_ids) = event_index.get_mut(event_type) {
                        rule_ids.retain(|id| id != rule_id);
                        if rule_ids.is_empty() {
                            event_index.remove(event_type);
                        }
                    }
                }
            }

            // Remove from table index
            {
                let mut table_index = self.table_index.write().await;
                for table in &rule.trigger_tables {
                    if let Some(rule_ids) = table_index.get_mut(table) {
                        rule_ids.retain(|id| id != rule_id);
                        if rule_ids.is_empty() {
                            table_index.remove(table);
                        }
                    }
                }
            }
        }

        drop(rules);

        // Remove rule
        self.rules.write().await.remove(rule_id);

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.active_rules = stats.active_rules.saturating_sub(1);
        }

        info!(rule_id = %rule_id, "Cache invalidation rule removed");

        Ok(())
    }

    /// Process an event and perform cache invalidation
    pub async fn process_event(&self, event: &Event) -> Result<Vec<InvalidationEvent>> {
        let start_time = std::time::Instant::now();

        // Find matching rules
        let matching_rules = self.find_matching_rules(event).await;

        if matching_rules.is_empty() {
            return Ok(Vec::new());
        }

        let mut invalidation_events = Vec::new();

        // Process each matching rule
        for rule in matching_rules {
            if let Ok(invalidation) = self.process_rule(&rule, event).await {
                invalidation_events.push(invalidation);
            }
        }

        // Update statistics
        let processing_time = start_time.elapsed().as_millis() as f64;
        {
            let mut stats = self.stats.write().await;
            stats.total_invalidations += invalidation_events.len() as u64;
            stats.avg_invalidation_time_ms = (stats.avg_invalidation_time_ms + processing_time) / 2.0;

            // Update event type stats
            let event_key = event.event_type.to_string();
            *stats.invalidations_by_event.entry(event_key).or_insert(0) += invalidation_events.len() as u64;
        }

        debug!(
            event_id = %event.id,
            invalidations = invalidation_events.len(),
            processing_time_ms = processing_time,
            "Processed cache invalidation event"
        );

        Ok(invalidation_events)
    }

    /// Find rules that match the given event
    async fn find_matching_rules(&self, event: &Event) -> Vec<InvalidationRule> {
        let mut matching_rules = Vec::new();

        // Check event type index
        let event_index = self.event_index.read().await;
        if let Some(rule_ids) = event_index.get(&event.event_type) {
            let rules = self.rules.read().await;
            for rule_id in rule_ids {
                if let Some(rule) = rules.get(rule_id) {
                    if rule.is_active && self.rule_matches_event(rule, event).await {
                        matching_rules.push(rule.clone());
                    }
                }
            }
        }

        // Check table index for database change events
        let table_name = self.extract_table_name_from_event(event);
        if let Some(table) = table_name {
            let table_index = self.table_index.read().await;
            if let Some(rule_ids) = table_index.get(&table) {
                let rules = self.rules.read().await;
                for rule_id in rule_ids {
                    if let Some(rule) = rules.get(rule_id) {
                        if rule.is_active && self.rule_matches_event(rule, event).await {
                            // Avoid duplicates
                            if !matching_rules.iter().any(|r| r.rule_id == rule.rule_id) {
                                matching_rules.push(rule.clone());
                            }
                        }
                    }
                }
            }
        }

        // Sort by priority (higher priority first)
        matching_rules.sort_by(|a, b| b.priority.cmp(&a.priority));

        matching_rules
    }

    /// Check if a rule matches an event
    async fn rule_matches_event(&self, rule: &InvalidationRule, event: &Event) -> bool {
        // Check organization isolation
        if rule.org_id != event.org_id {
            return false;
        }

        // Check trigger operations
        if !rule.trigger_operations.is_empty() {
            let operation = self.extract_operation_from_event(event);
            if let Some(op) = operation {
                if !rule.trigger_operations.contains(&op) {
                    return false;
                }
            }
        }

        // Apply conditions
        self.apply_invalidation_conditions(&rule.conditions, event).await
    }

    /// Extract table name from event
    fn extract_table_name_from_event(&self, event: &Event) -> Option<String> {
        match &event.data {
            EventData::DatabaseChange { table_name, .. } => Some(table_name.clone()),
            EventData::Database { table_name: Some(table), .. } => Some(table.clone()),
            _ => None,
        }
    }

    /// Extract operation from event
    fn extract_operation_from_event(&self, event: &Event) -> Option<String> {
        match &event.data {
            EventData::DatabaseChange { operation, .. } => Some(operation.clone()),
            EventData::Database { operation, .. } => Some(operation.clone()),
            _ => None,
        }
    }

    /// Apply invalidation conditions to determine if rule should fire
    async fn apply_invalidation_conditions(&self, conditions: &InvalidationConditions, event: &Event) -> bool {
        // Check rows affected
        if let Some(min_rows) = conditions.min_rows_affected {
            let rows_affected = self.extract_rows_affected_from_event(event);
            if rows_affected < min_rows {
                return false;
            }
        }

        // Check affected columns
        if !conditions.affected_columns.is_empty() {
            let event_columns = self.extract_affected_columns_from_event(event);
            let has_matching_column = conditions.affected_columns.iter().any(|col| {
                event_columns.contains(col)
            });
            if !has_matching_column {
                return false;
            }
        }

        // Apply time conditions
        for time_condition in &conditions.time_conditions {
            if !self.apply_time_condition(time_condition, event.timestamp) {
                return false;
            }
        }

        true
    }

    /// Extract rows affected from event
    fn extract_rows_affected_from_event(&self, event: &Event) -> u64 {
        match &event.data {
            EventData::DatabaseChange { rows_affected, .. } => *rows_affected,
            EventData::Database { rows_affected: Some(rows), .. } => *rows,
            _ => 0,
        }
    }

    /// Extract affected columns from event
    fn extract_affected_columns_from_event(&self, event: &Event) -> Vec<String> {
        match &event.data {
            EventData::DatabaseChange { new_values: Some(values), .. } => {
                values.keys().cloned().collect()
            }
            EventData::DatabaseChange { old_values: Some(values), .. } => {
                values.keys().cloned().collect()
            }
            _ => Vec::new(),
        }
    }

    /// Apply time condition
    fn apply_time_condition(&self, condition: &TimeCondition, timestamp: chrono::DateTime<chrono::Utc>) -> bool {
        match condition.condition_type {
            TimeConditionType::DuringHours => {
                // Parse hours like "09:00-17:00"
                if let Some((start_str, end_str)) = condition.value.split_once('-') {
                    if let (Ok(start_hour), Ok(end_hour)) = (start_str.parse::<u32>(), end_str.parse::<u32>()) {
                        let local_time = timestamp.with_timezone(&chrono::Local);
                        let current_hour = local_time.hour();
                        return current_hour >= start_hour && current_hour < end_hour;
                    }
                }
                true // Default to allow if parsing fails
            }
            TimeConditionType::After => {
                if let Ok(after_time) = chrono::DateTime::parse_from_rfc3339(&condition.value) {
                    timestamp > after_time.with_timezone(&chrono::Utc)
                } else {
                    true
                }
            }
            TimeConditionType::Before => {
                if let Ok(before_time) = chrono::DateTime::parse_from_rfc3339(&condition.value) {
                    timestamp < before_time.with_timezone(&chrono::Utc)
                } else {
                    true
                }
            }
            TimeConditionType::OnDays => {
                // Parse days like "Monday,Tuesday,Wednesday"
                let target_days: HashSet<&str> = condition.value.split(',').collect();
                let local_time = timestamp.with_timezone(&chrono::Local);
                let weekday = format!("{:?}", local_time.weekday());
                target_days.contains(weekday.as_str())
            }
        }
    }

    /// Process a rule and perform cache invalidation
    async fn process_rule(&self, rule: &InvalidationRule, event: &Event) -> Result<InvalidationEvent> {
        let mut cache_keys_to_invalidate = Vec::new();

        // Generate cache keys based on rule target categories
        for category in &rule.target_categories {
            let keys = self.generate_cache_keys_for_category(category, event, rule).await;
            cache_keys_to_invalidate.extend(keys);
        }

        // Add keys matching patterns
        for pattern in &rule.key_patterns {
            let keys = self.find_cache_keys_by_pattern(pattern, event.org_id).await?;
            cache_keys_to_invalidate.extend(keys);
        }

        // Perform invalidation
        let invalidated_count = if !cache_keys_to_invalidate.is_empty() {
            let count = self.cache_store.invalidate_batch(&cache_keys_to_invalidate).await?;

            // Update category stats
            {
                let mut stats = self.stats.write().await;
                for key in &cache_keys_to_invalidate {
                    let category_str = match &key.category {
                        CacheCategory::Query => "query",
                        CacheCategory::VectorSearch => "vector_search",
                        CacheCategory::Aggregation => "aggregation",
                        CacheCategory::TableMetadata => "table_metadata",
                        CacheCategory::IndexStats => "index_stats",
                        CacheCategory::CollectionProjection => "collection_projection",
                        CacheCategory::Custom(name) => name,
                    };
                    *stats.invalidations_by_category.entry(category_str.to_string()).or_insert(0) += 1;
                }
            }

            count
        } else {
            0
        };

        let invalidation_event = InvalidationEvent {
            trigger_event: event.clone(),
            cache_keys: cache_keys_to_invalidate,
            reason: format!("Rule '{}' triggered by {}", rule.rule_id, event.event_type),
            timestamp: chrono::Utc::now(),
        };

        debug!(
            rule_id = %rule.rule_id,
            event_id = %event.id,
            invalidated_keys = invalidated_count,
            "Cache invalidation completed"
        );

        Ok(invalidation_event)
    }

    /// Generate cache keys for a specific category
    async fn generate_cache_keys_for_category(
        &self,
        category: &CacheCategory,
        event: &Event,
        _rule: &InvalidationRule,
    ) -> Vec<CacheKey> {
        let mut keys = Vec::new();

        match category {
            CacheCategory::Query => {
                // For query cache, invalidate all queries touching affected tables
                if let Some(table_name) = self.extract_table_name_from_event(event) {
                    // This would typically query the cache store for keys containing the table name
                    // For now, we'll create a pattern-based key
                    keys.push(CacheKey {
                        org_id: event.org_id,
                        category: CacheCategory::Query,
                        components: vec!["*".to_string(), table_name],
                        metadata: HashMap::new(),
                    });
                }
            }
            CacheCategory::VectorSearch => {
                // Invalidate vector search results for affected tables
                if let Some(table_name) = self.extract_table_name_from_event(event) {
                    keys.push(CacheKey {
                        org_id: event.org_id,
                        category: CacheCategory::VectorSearch,
                        components: vec![table_name, "*".to_string(), "*".to_string()],
                        metadata: HashMap::new(),
                    });
                }
            }
            CacheCategory::Aggregation => {
                // Invalidate aggregation results for affected tables
                if let Some(table_name) = self.extract_table_name_from_event(event) {
                    keys.push(CacheKey {
                        org_id: event.org_id,
                        category: CacheCategory::Aggregation,
                        components: vec![table_name, "*".to_string()],
                        metadata: HashMap::new(),
                    });
                }
            }
            CacheCategory::TableMetadata => {
                // Invalidate table metadata for schema changes
                if matches!(event.event_type, EventType::SchemaChanged | EventType::TableCreated | EventType::TableDropped) {
                    if let Some(table_name) = self.extract_table_name_from_event(event) {
                        keys.push(CacheKey {
                            org_id: event.org_id,
                            category: CacheCategory::TableMetadata,
                            components: vec![table_name],
                            metadata: HashMap::new(),
                        });
                    }
                }
            }
            CacheCategory::IndexStats => {
                // Invalidate index statistics for data changes
                if matches!(event.event_type, EventType::RowInserted | EventType::RowUpdated | EventType::RowDeleted) {
                    if let Some(table_name) = self.extract_table_name_from_event(event) {
                        keys.push(CacheKey {
                            org_id: event.org_id,
                            category: CacheCategory::IndexStats,
                            components: vec![table_name, "*".to_string()],
                            metadata: HashMap::new(),
                        });
                    }
                }
            }
            CacheCategory::CollectionProjection => {
                // Invalidate collection projections for data changes
                if let Some(table_name) = self.extract_table_name_from_event(event) {
                    keys.push(CacheKey {
                        org_id: event.org_id,
                        category: CacheCategory::CollectionProjection,
                        components: vec![table_name],
                        metadata: HashMap::new(),
                    });
                }
            }
            CacheCategory::Custom(name) => {
                // Custom category invalidation logic would go here
                keys.push(CacheKey {
                    org_id: event.org_id,
                    category: CacheCategory::Custom(name.clone()),
                    components: vec!["*".to_string()],
                    metadata: HashMap::new(),
                });
            }
        }

        keys
    }

    /// Find cache keys by pattern
    async fn find_cache_keys_by_pattern(&self, pattern: &str, org_id: u64) -> Result<Vec<CacheKey>> {
        // This would typically query the cache store for matching keys
        // For now, we'll create a pattern-based approach
        let _invalidated = self.cache_store.invalidate_pattern(pattern, org_id).await?;

        // Return empty vec since we can't easily enumerate keys from pattern
        // In a real implementation, this would return the actual keys that were invalidated
        Ok(Vec::new())
    }

    /// Get invalidation statistics
    pub async fn get_stats(&self) -> InvalidationStats {
        let mut stats = self.stats.read().await.clone();

        // Update cache hit rate from cache store
        if let Ok(cache_stats) = self.cache_store.get_stats().await {
            let total_requests = cache_stats.hit_count + cache_stats.miss_count;
            if total_requests > 0 {
                stats.cache_hit_rate = cache_stats.hit_count as f64 / total_requests as f64;
            }
        }

        stats
    }

    /// Get active rules for an organization
    pub async fn get_rules_for_org(&self, org_id: u64) -> Vec<InvalidationRule> {
        self.rules
            .read()
            .await
            .values()
            .filter(|rule| rule.org_id == org_id)
            .cloned()
            .collect()
    }
}

/// Event handler implementation for cache invalidation
#[async_trait::async_trait]
impl EventHandler for CacheInvalidationManager {
    async fn handle(&self, event: &Event) -> HandlerResult {
        match self.process_event(event).await {
            Ok(invalidations) => {
                if !invalidations.is_empty() {
                    debug!(
                        event_id = %event.id,
                        invalidations = invalidations.len(),
                        "Cache invalidation successful"
                    );
                }
                HandlerResult::Success
            }
            Err(e) => {
                warn!(
                    event_id = %event.id,
                    error = %e,
                    "Cache invalidation failed"
                );
                HandlerResult::Retry
            }
        }
    }

    fn interested_in(&self, event_type: &EventType) -> bool {
        // Interested in database change events and transaction events
        matches!(
            event_type,
            EventType::TransactionCommitted |
            EventType::RowInserted |
            EventType::RowUpdated |
            EventType::RowDeleted |
            EventType::SchemaChanged |
            EventType::TableCreated |
            EventType::TableDropped |
            EventType::IndexBuilt |
            EventType::IndexDropped |
            EventType::DataIngested
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// Mock cache store for testing
    struct MockCacheStore {
        entries: Mutex<HashSet<String>>,
    }

    impl MockCacheStore {
        fn new() -> Self {
            Self {
                entries: Mutex::new(HashSet::new()),
            }
        }

        fn add_entry(&self, key: &str) {
            self.entries.lock().unwrap().insert(key.to_string());
        }
    }

    #[async_trait]
    impl CacheStore for MockCacheStore {
        async fn invalidate(&self, key: &CacheKey) -> Result<bool> {
            let key_str = key.to_string();
            Ok(self.entries.lock().unwrap().remove(&key_str))
        }

        async fn invalidate_batch(&self, keys: &[CacheKey]) -> Result<u64> {
            let mut count = 0;
            let mut entries = self.entries.lock().unwrap();
            for key in keys {
                let key_str = key.to_string();
                if entries.remove(&key_str) {
                    count += 1;
                }
            }
            Ok(count)
        }

        async fn invalidate_pattern(&self, pattern: &str, _org_id: u64) -> Result<u64> {
            let mut count = 0;
            let mut entries = self.entries.lock().unwrap();

            let keys_to_remove: Vec<String> = entries
                .iter()
                .filter(|key| {
                    if pattern.contains('*') {
                        let regex_pattern = pattern.replace('*', ".*");
                        if let Ok(regex) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
                            regex.is_match(key)
                        } else {
                            false
                        }
                    } else {
                        key.as_str() == pattern
                    }
                })
                .cloned()
                .collect();

            for key in keys_to_remove {
                entries.remove(&key);
                count += 1;
            }

            Ok(count)
        }

        async fn get_stats(&self) -> Result<CacheStats> {
            Ok(CacheStats {
                total_entries: self.entries.lock().unwrap().len() as u64,
                total_size_bytes: 0,
                hit_count: 100,
                miss_count: 20,
                eviction_count: 0,
            })
        }

        async fn exists(&self, key: &CacheKey) -> Result<bool> {
            let key_str = key.to_string();
            Ok(self.entries.lock().unwrap().contains(&key_str))
        }
    }

    #[tokio::test]
    async fn test_cache_key_creation() {
        let key = CacheKey::query(1, "SELECT * FROM users", vec!["param1".to_string()]);
        assert_eq!(key.org_id, 1);
        assert_eq!(key.category, CacheCategory::Query);
        assert_eq!(key.components, vec!["SELECT * FROM users", "param1"]);
    }

    #[tokio::test]
    async fn test_cache_key_pattern_matching() {
        let key = CacheKey::query(1, "SELECT * FROM users", vec![]);

        assert!(key.matches_pattern("*"));
        assert!(key.matches_pattern("1:query:*"));
        assert!(!key.matches_pattern("2:query:*"));
    }

    #[tokio::test]
    async fn test_invalidation_rule_creation() {
        let rule = InvalidationRule {
            rule_id: "test_rule".to_string(),
            org_id: 1,
            trigger_events: HashSet::from([EventType::RowInserted]),
            trigger_tables: HashSet::from(["users".to_string()]),
            trigger_operations: HashSet::from(["INSERT".to_string()]),
            target_categories: HashSet::from([CacheCategory::Query]),
            key_patterns: vec!["*:query:*users*".to_string()],
            conditions: InvalidationConditions::default(),
            is_active: true,
            priority: 10,
            created_at: chrono::Utc::now(),
        };

        assert_eq!(rule.rule_id, "test_rule");
        assert!(rule.is_active);
    }

    #[tokio::test]
    async fn test_cache_invalidation_manager() {
        let cache_store = Arc::new(MockCacheStore::new());
        let manager = CacheInvalidationManager::new(cache_store.clone());

        // Add a cache entry
        cache_store.add_entry("1:query:SELECT * FROM users");

        // Add an invalidation rule
        let rule = InvalidationRule {
            rule_id: "test_rule".to_string(),
            org_id: 1,
            trigger_events: HashSet::from([EventType::RowInserted]),
            trigger_tables: HashSet::from(["users".to_string()]),
            trigger_operations: HashSet::from(["INSERT".to_string()]),
            target_categories: HashSet::from([CacheCategory::Query]),
            key_patterns: vec!["*:query:*users*".to_string()],
            conditions: InvalidationConditions::default(),
            is_active: true,
            priority: 10,
            created_at: chrono::Utc::now(),
        };

        manager.add_rule(rule).await.unwrap();

        // Create a test event
        let event = zevents::Event::new(
            EventType::RowInserted,
            zevents::EventData::DatabaseChange {
                org_id: 1,
                table_name: "users".to_string(),
                operation: "INSERT".to_string(),
                transaction_id: None,
                user_id: None,
                connection_id: None,
                old_values: None,
                new_values: None,
                where_clause: None,
                rows_affected: 1,
                schema_changes: None,
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
            },
            1,
        );

        // Process the event
        let invalidations = manager.process_event(&event).await.unwrap();
        assert!(!invalidations.is_empty());

        // Check stats
        let stats = manager.get_stats().await;
        assert!(stats.total_invalidations > 0);
    }
}