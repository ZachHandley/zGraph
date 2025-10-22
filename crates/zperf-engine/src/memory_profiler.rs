//! Advanced memory profiling and analysis for performance benchmarks
//!
//! This module provides comprehensive memory profiling capabilities including:
//! - Real-time memory usage tracking
//! - Allocation pattern analysis
//! - Memory leak detection
//! - Peak memory usage monitoring
//! - Memory efficiency metrics

use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

/// Memory profiling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryProfilerConfig {
    /// Enable detailed allocation tracking
    pub track_allocations: bool,
    /// Sample memory usage at this interval
    pub sampling_interval_ms: u64,
    /// Maximum number of allocation samples to store
    pub max_samples: usize,
    /// Track memory fragmentation
    pub track_fragmentation: bool,
    /// Enable leak detection
    pub detect_leaks: bool,
}

impl Default for MemoryProfilerConfig {
    fn default() -> Self {
        Self {
            track_allocations: true,
            sampling_interval_ms: 10, // 10ms sampling
            max_samples: 10000,
            track_fragmentation: false, // Expensive operation
            detect_leaks: true,
        }
    }
}

/// Memory usage snapshot at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySnapshot {
    /// Timestamp of the snapshot
    pub timestamp: u64,
    /// Resident Set Size (RSS) in bytes
    pub rss_bytes: usize,
    /// Virtual memory size in bytes
    pub virtual_bytes: usize,
    /// Heap allocated bytes (if available)
    pub heap_bytes: usize,
    /// Number of allocations since start
    pub allocation_count: usize,
    /// Memory fragmentation percentage (0-100)
    pub fragmentation_percent: f32,
}

/// Memory allocation event for detailed tracking
#[derive(Debug, Clone)]
pub struct AllocationEvent {
    /// When this allocation event occurred
    pub timestamp: Instant,
    /// Size of the allocation in bytes
    pub size: usize,
    /// Memory alignment requirement
    pub alignment: usize,
    /// True for allocation, false for deallocation
    pub is_allocation: bool,
    /// Simplified stack trace identifier
    pub stack_trace_hash: u64,
}

/// Memory profiling results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryProfileResult {
    /// Configuration used for profiling
    pub config: MemoryProfilerConfig,
    /// Duration of profiling session
    pub duration_ms: u64,
    /// Memory snapshots over time
    pub snapshots: Vec<MemorySnapshot>,
    /// Peak memory usage
    pub peak_rss_bytes: usize,
    /// Average memory usage
    pub average_rss_bytes: usize,
    /// Total allocations during session
    pub total_allocations: usize,
    /// Total deallocations during session
    pub total_deallocations: usize,
    /// Net memory leaked (allocations - deallocations)
    pub net_leaked_bytes: i64,
    /// Memory efficiency score (0-100)
    pub efficiency_score: f32,
    /// Detected memory leaks
    pub potential_leaks: Vec<MemoryLeak>,
}

/// Potential memory leak information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryLeak {
    /// Hash identifying the stack trace where leak occurred
    pub stack_trace_hash: u64,
    /// Total bytes leaked at this location
    pub leaked_bytes: usize,
    /// Number of allocations that weren't freed
    pub allocation_count: usize,
    /// Timestamp of first leak detection
    pub first_seen: u64,
    /// Timestamp of most recent leak detection
    pub last_seen: u64,
}

/// Advanced memory profiler
pub struct MemoryProfiler {
    config: MemoryProfilerConfig,
    start_time: Instant,
    snapshots: Arc<Mutex<Vec<MemorySnapshot>>>,
    allocation_events: Arc<Mutex<Vec<AllocationEvent>>>,
    allocation_tracker: Arc<RwLock<HashMap<u64, AllocationInfo>>>,
    is_profiling: Arc<AtomicBool>,
    total_allocations: Arc<AtomicUsize>,
    total_deallocations: Arc<AtomicUsize>,
}

use std::sync::atomic::AtomicBool;

#[derive(Debug, Clone)]
struct AllocationInfo {
    size: usize,
    timestamp: Instant,
    stack_trace_hash: u64,
}

impl MemoryProfiler {
    /// Create a new memory profiler
    pub fn new(config: MemoryProfilerConfig) -> Self {
        Self {
            config,
            start_time: Instant::now(),
            snapshots: Arc::new(Mutex::new(Vec::new())),
            allocation_events: Arc::new(Mutex::new(Vec::new())),
            allocation_tracker: Arc::new(RwLock::new(HashMap::new())),
            is_profiling: Arc::new(AtomicBool::new(false)),
            total_allocations: Arc::new(AtomicUsize::new(0)),
            total_deallocations: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Start memory profiling
    pub fn start_profiling(&self) {
        self.is_profiling.store(true, Ordering::SeqCst);

        // Start memory sampling thread
        if self.config.sampling_interval_ms > 0 {
            self.start_sampling_thread();
        }

        // Take initial snapshot
        if let Some(snapshot) = self.take_memory_snapshot() {
            if let Ok(mut snapshots) = self.snapshots.lock() {
                snapshots.push(snapshot);
            }
        }
    }

    /// Stop memory profiling and return results
    pub fn stop_profiling(&self) -> MemoryProfileResult {
        self.is_profiling.store(false, Ordering::SeqCst);

        // Take final snapshot
        if let Some(snapshot) = self.take_memory_snapshot() {
            if let Ok(mut snapshots) = self.snapshots.lock() {
                snapshots.push(snapshot);
            }
        }

        self.generate_report()
    }

    /// Start background thread for memory sampling
    fn start_sampling_thread(&self) {
        let snapshots = Arc::clone(&self.snapshots);
        let is_profiling = Arc::clone(&self.is_profiling);
        let interval = Duration::from_millis(self.config.sampling_interval_ms);
        let max_samples = self.config.max_samples;

        std::thread::spawn(move || {
            while is_profiling.load(Ordering::SeqCst) {
                if let Some(snapshot) = Self::take_system_memory_snapshot() {
                    if let Ok(mut snapshots_guard) = snapshots.lock() {
                        snapshots_guard.push(snapshot);

                        // Limit the number of samples to prevent memory bloat
                        if snapshots_guard.len() > max_samples {
                            snapshots_guard.remove(0);
                        }
                    }
                }

                std::thread::sleep(interval);
            }
        });
    }

    /// Take a memory snapshot
    fn take_memory_snapshot(&self) -> Option<MemorySnapshot> {
        Self::take_system_memory_snapshot()
    }

    /// Take system memory snapshot using platform APIs
    fn take_system_memory_snapshot() -> Option<MemorySnapshot> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_millis() as u64;

        // Platform-specific memory measurement
        #[cfg(target_os = "linux")]
        {
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                let mut rss_bytes = 0;
                let mut virtual_bytes = 0;

                for line in status.lines() {
                    if line.starts_with("VmRSS:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<usize>() {
                                rss_bytes = kb * 1024;
                            }
                        }
                    } else if line.starts_with("VmSize:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<usize>() {
                                virtual_bytes = kb * 1024;
                            }
                        }
                    }
                }

                return Some(MemorySnapshot {
                    timestamp,
                    rss_bytes,
                    virtual_bytes,
                    heap_bytes: Self::estimate_heap_usage(),
                    allocation_count: 0, // Would need custom allocator integration
                    fragmentation_percent: 0.0, // Expensive to calculate
                });
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::mem;

            extern "C" {
                fn mach_task_self() -> u32;
                fn task_info(
                    target_task: u32,
                    flavor: u32,
                    task_info_out: *mut u8,
                    task_info_outCnt: *mut u32,
                ) -> i32;
            }

            const TASK_BASIC_INFO: u32 = 5;
            const TASK_BASIC_INFO_COUNT: u32 = 5;

            #[repr(C)]
            struct TaskBasicInfo {
                virtual_size: u32,
                resident_size: u32,
                user_time: [u32; 2],
                system_time: [u32; 2],
                policy: u32,
            }

            unsafe {
                let mut info: TaskBasicInfo = mem::zeroed();
                let mut count = TASK_BASIC_INFO_COUNT;
                let result = task_info(
                    mach_task_self(),
                    TASK_BASIC_INFO,
                    &mut info as *mut _ as *mut u8,
                    &mut count,
                );

                if result == 0 {
                    return Some(MemorySnapshot {
                        timestamp,
                        rss_bytes: info.resident_size as usize,
                        virtual_bytes: info.virtual_size as usize,
                        heap_bytes: Self::estimate_heap_usage(),
                        allocation_count: 0,
                        fragmentation_percent: 0.0,
                    });
                }
            }
        }

        // Fallback for other platforms
        Some(MemorySnapshot {
            timestamp,
            rss_bytes: Self::estimate_heap_usage(),
            virtual_bytes: Self::estimate_heap_usage() * 2,
            heap_bytes: Self::estimate_heap_usage(),
            allocation_count: 0,
            fragmentation_percent: 0.0,
        })
    }

    /// Estimate heap usage (fallback method)
    fn estimate_heap_usage() -> usize {
        // Simple estimation based on allocator behavior
        // This is a rough estimate and should be replaced with
        // actual allocator integration in production
        2 * 1024 * 1024 // 2MB baseline
    }

    /// Record an allocation event
    pub fn record_allocation(&self, size: usize, alignment: usize, ptr: *mut u8) {
        if !self.is_profiling.load(Ordering::SeqCst) || !self.config.track_allocations {
            return;
        }

        let stack_trace_hash = self.get_stack_trace_hash();
        let event = AllocationEvent {
            timestamp: Instant::now(),
            size,
            alignment,
            is_allocation: true,
            stack_trace_hash,
        };

        // Record in allocation tracker
        if self.config.detect_leaks {
            let allocation_info = AllocationInfo {
                size,
                timestamp: Instant::now(),
                stack_trace_hash,
            };

            self.allocation_tracker
                .write()
                .insert(ptr as u64, allocation_info);
        }

        self.total_allocations.fetch_add(1, Ordering::SeqCst);

        // Store event if we're tracking detailed events
        if let Ok(mut events) = self.allocation_events.lock() {
            events.push(event);

            // Limit event storage to prevent memory bloat
            if events.len() > self.config.max_samples {
                events.remove(0);
            }
        }
    }

    /// Record a deallocation event
    pub fn record_deallocation(&self, ptr: *mut u8) {
        if !self.is_profiling.load(Ordering::SeqCst) || !self.config.track_allocations {
            return;
        }

        // Remove from allocation tracker
        if self.config.detect_leaks {
            self.allocation_tracker.write().remove(&(ptr as u64));
        }

        self.total_deallocations.fetch_add(1, Ordering::SeqCst);
    }

    /// Generate stack trace hash (simplified)
    fn get_stack_trace_hash(&self) -> u64 {
        // In a real implementation, this would capture and hash
        // the actual stack trace. For benchmarks, we use a simple hash.
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        Instant::now().elapsed().as_nanos().hash(&mut hasher);
        hasher.finish()
    }

    /// Generate comprehensive profiling report
    fn generate_report(&self) -> MemoryProfileResult {
        let duration_ms = self.start_time.elapsed().as_millis() as u64;
        let snapshots = self.snapshots.lock().unwrap().clone();

        // Calculate statistics
        let peak_rss_bytes = snapshots.iter()
            .map(|s| s.rss_bytes)
            .max()
            .unwrap_or(0);

        let average_rss_bytes = if snapshots.is_empty() {
            0
        } else {
            snapshots.iter()
                .map(|s| s.rss_bytes)
                .sum::<usize>() / snapshots.len()
        };

        let total_allocations = self.total_allocations.load(Ordering::SeqCst);
        let total_deallocations = self.total_deallocations.load(Ordering::SeqCst);
        let net_leaked_bytes = (total_allocations as i64) - (total_deallocations as i64);

        // Detect potential memory leaks
        let potential_leaks = self.detect_memory_leaks();

        // Calculate efficiency score
        let efficiency_score = self.calculate_efficiency_score(
            &snapshots,
            total_allocations,
            potential_leaks.len(),
        );

        MemoryProfileResult {
            config: self.config.clone(),
            duration_ms,
            snapshots,
            peak_rss_bytes,
            average_rss_bytes,
            total_allocations,
            total_deallocations,
            net_leaked_bytes,
            efficiency_score,
            potential_leaks,
        }
    }

    /// Detect potential memory leaks
    fn detect_memory_leaks(&self) -> Vec<MemoryLeak> {
        if !self.config.detect_leaks {
            return Vec::new();
        }

        let tracker = self.allocation_tracker.read();
        let mut leak_groups: HashMap<u64, Vec<&AllocationInfo>> = HashMap::new();

        // Group allocations by stack trace
        for (_, allocation_info) in tracker.iter() {
            leak_groups
                .entry(allocation_info.stack_trace_hash)
                .or_default()
                .push(allocation_info);
        }

        // Convert to leak reports
        leak_groups
            .into_iter()
            .filter_map(|(stack_trace_hash, allocations)| {
                if allocations.is_empty() {
                    return None;
                }

                let leaked_bytes: usize = allocations.iter().map(|a| a.size).sum();
                let first_seen = allocations.iter()
                    .map(|a| a.timestamp)
                    .min()?
                    .elapsed()
                    .as_millis() as u64;
                let last_seen = allocations.iter()
                    .map(|a| a.timestamp)
                    .max()?
                    .elapsed()
                    .as_millis() as u64;

                Some(MemoryLeak {
                    stack_trace_hash,
                    leaked_bytes,
                    allocation_count: allocations.len(),
                    first_seen,
                    last_seen,
                })
            })
            .collect()
    }

    /// Calculate memory efficiency score (0-100)
    fn calculate_efficiency_score(
        &self,
        snapshots: &[MemorySnapshot],
        allocations: usize,
        leak_count: usize,
    ) -> f32 {
        if snapshots.is_empty() {
            return 0.0;
        }

        // Factors for efficiency scoring:
        // 1. Memory stability (low variance in usage)
        // 2. Low peak-to-average ratio
        // 3. Few memory leaks
        // 4. Reasonable allocation patterns

        let rss_values: Vec<f64> = snapshots.iter()
            .map(|s| s.rss_bytes as f64)
            .collect();

        let mean = rss_values.iter().sum::<f64>() / rss_values.len() as f64;
        let variance = rss_values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / rss_values.len() as f64;
        let std_dev = variance.sqrt();

        // Memory stability score (lower variance = better)
        let stability_score = if mean > 0.0 {
            100.0 - (std_dev / mean * 100.0).min(100.0)
        } else {
            0.0
        };

        // Peak efficiency (lower peak-to-average ratio = better)
        let peak = rss_values.iter().fold(0.0f64, |a, &b| a.max(b));
        let peak_efficiency = if peak > 0.0 {
            (mean / peak * 100.0).min(100.0)
        } else {
            100.0
        };

        // Leak penalty
        let leak_penalty = (leak_count as f32 * 10.0).min(50.0);

        // Allocation efficiency (penalize excessive allocations)
        let alloc_efficiency = if allocations > 1000 {
            (100.0 - ((allocations as f32 - 1000.0) / 100.0)).max(0.0)
        } else {
            100.0
        };

        // Weighted average of all factors
        let total_score = (stability_score * 0.3 +
                          peak_efficiency * 0.3 +
                          (100.0 - leak_penalty as f64) * 0.2 +
                          alloc_efficiency as f64 * 0.2) as f32;

        total_score.max(0.0).min(100.0)
    }
}

/// Convenience function to profile a closure's memory usage
pub fn profile_memory<T, F>(config: MemoryProfilerConfig, f: F) -> (T, MemoryProfileResult)
where
    F: FnOnce() -> T,
{
    let profiler = MemoryProfiler::new(config);
    profiler.start_profiling();

    let result = f();

    let profile_result = profiler.stop_profiling();
    (result, profile_result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_profiler_basic() {
        let config = MemoryProfilerConfig::default();
        let profiler = MemoryProfiler::new(config);

        profiler.start_profiling();

        // Simulate some allocations
        let _data: Vec<u8> = vec![0; 1024 * 1024]; // 1MB

        std::thread::sleep(Duration::from_millis(50));

        let result = profiler.stop_profiling();

        assert!(result.duration_ms > 0);
        assert!(!result.snapshots.is_empty());
        assert!(result.efficiency_score >= 0.0 && result.efficiency_score <= 100.0);
    }

    #[test]
    fn test_profile_memory_function() {
        let config = MemoryProfilerConfig {
            sampling_interval_ms: 5,
            ..Default::default()
        };

        let (result, profile) = profile_memory(config, || {
            // Simulate memory-intensive operation
            let data: Vec<Vec<u8>> = (0..100)
                .map(|_| vec![0u8; 1024])
                .collect();
            data.len()
        });

        assert_eq!(result, 100);
        assert!(profile.peak_rss_bytes > 0);
        assert!(!profile.snapshots.is_empty());
    }
}