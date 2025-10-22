//! Memory allocator optimization and monitoring
//!
//! This module provides optimized memory allocation using mimalloc and comprehensive
//! allocator performance monitoring for database workloads.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use anyhow::Result;

// Note: mimalloc integration moved to global allocator in lib.rs

/// Allocator performance statistics
#[derive(Debug, Clone)]
pub struct AllocatorStats {
    /// Total bytes allocated
    pub bytes_allocated: u64,
    /// Total bytes deallocated
    pub bytes_deallocated: u64,
    /// Current bytes in use
    pub bytes_in_use: u64,
    /// Peak memory usage
    pub peak_usage: u64,
    /// Number of allocation calls
    pub allocation_count: usize,
    /// Number of deallocation calls
    pub deallocation_count: usize,
    /// Average allocation size
    pub avg_allocation_size: f64,
}

impl Default for AllocatorStats {
    fn default() -> Self {
        Self {
            bytes_allocated: 0,
            bytes_deallocated: 0,
            bytes_in_use: 0,
            peak_usage: 0,
            allocation_count: 0,
            deallocation_count: 0,
            avg_allocation_size: 0.0,
        }
    }
}

/// Global allocator statistics tracking
static GLOBAL_ALLOCATOR_STATS: AllocatorStatsTracker = AllocatorStatsTracker::new();

struct AllocatorStatsTracker {
    bytes_allocated: AtomicU64,
    bytes_deallocated: AtomicU64,
    peak_usage: AtomicU64,
    allocation_count: AtomicUsize,
    deallocation_count: AtomicUsize,
}

impl AllocatorStatsTracker {
    const fn new() -> Self {
        Self {
            bytes_allocated: AtomicU64::new(0),
            bytes_deallocated: AtomicU64::new(0),
            peak_usage: AtomicU64::new(0),
            allocation_count: AtomicUsize::new(0),
            deallocation_count: AtomicUsize::new(0),
        }
    }

    fn record_allocation(&self, size: usize) {
        let size_u64 = size as u64;
        let prev_allocated = self.bytes_allocated.fetch_add(size_u64, Ordering::Relaxed);
        let new_allocated = prev_allocated + size_u64;

        self.allocation_count.fetch_add(1, Ordering::Relaxed);

        // Update peak usage
        let current_deallocated = self.bytes_deallocated.load(Ordering::Relaxed);
        let current_in_use = new_allocated.saturating_sub(current_deallocated);

        let mut current_peak = self.peak_usage.load(Ordering::Relaxed);
        while current_in_use > current_peak {
            match self.peak_usage.compare_exchange_weak(
                current_peak,
                current_in_use,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_peak) => current_peak = new_peak,
            }
        }
    }

    fn record_deallocation(&self, size: usize) {
        self.bytes_deallocated.fetch_add(size as u64, Ordering::Relaxed);
        self.deallocation_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_stats(&self) -> AllocatorStats {
        let bytes_allocated = self.bytes_allocated.load(Ordering::Relaxed);
        let bytes_deallocated = self.bytes_deallocated.load(Ordering::Relaxed);
        let allocation_count = self.allocation_count.load(Ordering::Relaxed);

        AllocatorStats {
            bytes_allocated,
            bytes_deallocated,
            bytes_in_use: bytes_allocated.saturating_sub(bytes_deallocated),
            peak_usage: self.peak_usage.load(Ordering::Relaxed),
            allocation_count,
            deallocation_count: self.deallocation_count.load(Ordering::Relaxed),
            avg_allocation_size: if allocation_count > 0 {
                bytes_allocated as f64 / allocation_count as f64
            } else {
                0.0
            },
        }
    }

    fn reset(&self) {
        self.bytes_allocated.store(0, Ordering::Relaxed);
        self.bytes_deallocated.store(0, Ordering::Relaxed);
        self.peak_usage.store(0, Ordering::Relaxed);
        self.allocation_count.store(0, Ordering::Relaxed);
        self.deallocation_count.store(0, Ordering::Relaxed);
    }
}

/// Initialize allocator monitoring
pub fn init_allocator_monitoring() -> Result<()> {
    #[cfg(feature = "mimalloc")]
    {
        // Configure mimalloc for database workloads
        configure_mimalloc_for_database();
    }

    // Reset statistics
    GLOBAL_ALLOCATOR_STATS.reset();

    Ok(())
}

/// Configure mimalloc specifically for database workloads
#[cfg(feature = "mimalloc")]
fn configure_mimalloc_for_database() {
    // These configurations are applied through environment variables
    // In a real deployment, these would be set before the process starts

    // Enable eager page commit for better performance with large allocations
    std::env::set_var("MIMALLOC_EAGER_COMMIT", "1");

    // Use large OS pages when available (better for large data structures)
    std::env::set_var("MIMALLOC_LARGE_OS_PAGES", "1");

    // Reserve more virtual memory to reduce fragmentation
    std::env::set_var("MIMALLOC_RESERVE_HUGE_OS_PAGES", "1");

    // Optimize for database-style allocation patterns
    std::env::set_var("MIMALLOC_ARENA_EAGER_COMMIT", "2");
}

/// Get current allocator statistics
pub fn get_allocator_stats() -> AllocatorStats {
    GLOBAL_ALLOCATOR_STATS.get_stats()
}

/// Reset allocator statistics
pub fn reset_allocator_stats() {
    GLOBAL_ALLOCATOR_STATS.reset();
}

/// Record an allocation for statistics tracking
pub fn record_allocation(_size: usize) {
    #[cfg(feature = "allocator-stats")]
    GLOBAL_ALLOCATOR_STATS.record_allocation(_size);
}

/// Record a deallocation for statistics tracking
pub fn record_deallocation(_size: usize) {
    #[cfg(feature = "allocator-stats")]
    GLOBAL_ALLOCATOR_STATS.record_deallocation(_size);
}

/// Database-optimized allocator wrapper
pub struct DatabaseAllocator {
    stats: Arc<AllocatorStatsTracker>,
}

impl DatabaseAllocator {
    /// Create a new database-optimized allocator
    pub fn new() -> Self {
        Self {
            stats: Arc::new(AllocatorStatsTracker::new()),
        }
    }

    /// Allocate memory with statistics tracking
    pub fn allocate(&self, size: usize) -> Result<*mut u8> {
        let layout = std::alloc::Layout::from_size_align(size, 8)?;
        let ptr = unsafe { std::alloc::alloc(layout) };

        if ptr.is_null() {
            return Err(anyhow::anyhow!("Failed to allocate {} bytes", size));
        }

        self.stats.record_allocation(size);
        Ok(ptr)
    }

    /// Deallocate memory with statistics tracking
    pub unsafe fn deallocate(&self, ptr: *mut u8, size: usize) {
        if !ptr.is_null() {
            unsafe {
                let layout = std::alloc::Layout::from_size_align_unchecked(size, 8);
                std::alloc::dealloc(ptr, layout);
            }
            self.stats.record_deallocation(size);
        }
    }

    /// Get allocator statistics
    pub fn get_stats(&self) -> AllocatorStats {
        self.stats.get_stats()
    }
}

impl Default for DatabaseAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII wrapper for tracked allocations
pub struct TrackedAllocation {
    ptr: *mut u8,
    size: usize,
    allocator: Arc<DatabaseAllocator>,
}

impl TrackedAllocation {
    /// Create a new tracked allocation
    pub fn new(size: usize, allocator: Arc<DatabaseAllocator>) -> Result<Self> {
        let ptr = allocator.allocate(size)?;
        Ok(Self {
            ptr,
            size,
            allocator,
        })
    }

    /// Get the raw pointer
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    /// Get the allocation size
    pub fn size(&self) -> usize {
        self.size
    }

    /// Convert to a byte slice
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    /// Convert to a mutable byte slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

impl Drop for TrackedAllocation {
    fn drop(&mut self) {
        unsafe {
            self.allocator.deallocate(self.ptr, self.size);
        }
    }
}

unsafe impl Send for TrackedAllocation {}
unsafe impl Sync for TrackedAllocation {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocator_stats_tracking() {
        let tracker = AllocatorStatsTracker::new();

        // Record some allocations
        tracker.record_allocation(1024);
        tracker.record_allocation(2048);

        let stats = tracker.get_stats();
        assert_eq!(stats.bytes_allocated, 3072);
        assert_eq!(stats.allocation_count, 2);
        assert!(stats.avg_allocation_size > 0.0);

        // Record deallocations
        tracker.record_deallocation(1024);
        let stats = tracker.get_stats();
        assert_eq!(stats.bytes_in_use, 2048);
        assert_eq!(stats.deallocation_count, 1);
    }

    #[test]
    fn test_database_allocator() {
        let allocator = DatabaseAllocator::new();

        // Test allocation and deallocation
        let ptr = allocator.allocate(1024).unwrap();
        assert!(!ptr.is_null());

        let stats = allocator.get_stats();
        assert_eq!(stats.allocation_count, 1);
        assert_eq!(stats.bytes_allocated, 1024);

        unsafe { allocator.deallocate(ptr, 1024); }
        let stats = allocator.get_stats();
        assert_eq!(stats.deallocation_count, 1);
        assert_eq!(stats.bytes_in_use, 0);
    }

    #[test]
    fn test_tracked_allocation() {
        let allocator = Arc::new(DatabaseAllocator::new());

        {
            let allocation = TrackedAllocation::new(512, allocator.clone()).unwrap();
            assert_eq!(allocation.size(), 512);
            assert!(!allocation.as_ptr().is_null());

            let stats = allocator.get_stats();
            assert_eq!(stats.allocation_count, 1);
        } // allocation dropped here

        let stats = allocator.get_stats();
        assert_eq!(stats.deallocation_count, 1);
        assert_eq!(stats.bytes_in_use, 0);
    }
}