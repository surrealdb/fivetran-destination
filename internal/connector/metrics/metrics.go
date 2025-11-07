package metrics

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/surrealdb/fivetran-destination/internal/connector/framework"
)

// Collector collects performance metrics for the connector
type Collector struct {
	mu sync.RWMutex

	// Record processing metrics
	recordsProcessed atomic.Int64
	recordsPerSecond float64
	bytesProcessed   atomic.Int64
	bytesPerSecond   float64

	// File processing metrics
	filesProcessed        atomic.Int64
	currentFileProcessing atomic.Int32
	totalFileProcessing   atomic.Int64
	fileProcessingErrors  atomic.Int64

	// Database write metrics
	dbWritesCompleted atomic.Int64
	dbWritesPerSecond float64
	dbWriteErrors     atomic.Int64

	// Timing metrics
	lastResetTime    time.Time
	totalProcessTime atomic.Int64 // in nanoseconds

	// Resource usage
	cpuUsagePercent float64
	memoryUsageMB   uint64
	goroutineCount  int

	// Configuration
	LogInterval time.Duration
	logging     framework.Logger
}

// NewCollector creates a new metrics collector
func NewCollector(logging framework.Logger, logInterval time.Duration) *Collector {
	if logInterval <= 0 {
		logInterval = 30 * time.Second // default to 30 seconds
	}

	mc := &Collector{
		lastResetTime: time.Now(),
		LogInterval:   logInterval,
		logging:       logging,
	}

	return mc
}

// Start begins periodic metrics logging
func (mc *Collector) Start(ctx context.Context) {
	go mc.periodicLogger(ctx)
	go mc.resourceMonitor(ctx)
}

// RecordProcessed increments the processed records counter
func (mc *Collector) RecordProcessed(count int64, bytes int64) {
	mc.recordsProcessed.Add(count)
	mc.bytesProcessed.Add(bytes)
}

// FileProcessed increments the processed files counter
func (mc *Collector) FileProcessed() {
	mc.filesProcessed.Add(1)
}

// FileProcessingStarted increments the current file processing counter
func (mc *Collector) FileProcessingStarted() {
	mc.currentFileProcessing.Add(1)
	mc.totalFileProcessing.Add(1)
}

// FileProcessingCompleted decrements the current file processing counter
func (mc *Collector) FileProcessingCompleted(duration time.Duration) {
	mc.currentFileProcessing.Add(-1)
	mc.totalProcessTime.Add(duration.Nanoseconds())
}

// FileProcessingError increments the file processing error counter
func (mc *Collector) FileProcessingError() {
	mc.fileProcessingErrors.Add(1)
}

// DBWriteCompleted increments the database write counter
func (mc *Collector) DBWriteCompleted(count int64) {
	mc.dbWritesCompleted.Add(count)
}

// DBWriteError increments the database write error counter
func (mc *Collector) DBWriteError() {
	mc.dbWriteErrors.Add(1)
}

// periodicLogger logs metrics at regular intervals
func (mc *Collector) periodicLogger(ctx context.Context) {
	ticker := time.NewTicker(mc.LogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mc.logMetrics()
		}
	}
}

// resourceMonitor updates resource usage metrics
func (mc *Collector) resourceMonitor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second) // update every 5 seconds
	defer ticker.Stop()

	var lastCPU runtime.MemStats
	runtime.ReadMemStats(&lastCPU)
	lastTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mc.updateResourceMetrics(&lastCPU, lastTime)
			lastTime = time.Now()
		}
	}
}

// updateResourceMetrics updates CPU and memory usage metrics
func (mc *Collector) updateResourceMetrics(lastStats *runtime.MemStats, lastTime time.Time) {
	var currentStats runtime.MemStats
	runtime.ReadMemStats(&currentStats)

	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Calculate memory usage
	mc.memoryUsageMB = currentStats.Alloc / 1024 / 1024
	mc.goroutineCount = runtime.NumGoroutine()

	// CPU usage approximation based on GC stats
	// This is not perfect but gives a rough idea
	timeDiff := time.Since(lastTime).Seconds()
	if timeDiff > 0 {
		gcTime := float64(currentStats.PauseTotalNs-lastStats.PauseTotalNs) / 1e9
		mc.cpuUsagePercent = (gcTime / timeDiff) * 100
	}

	*lastStats = currentStats
}

// logMetrics logs the current metrics
func (mc *Collector) logMetrics() {
	elapsed := time.Since(mc.lastResetTime).Seconds()
	if elapsed <= 0 {
		return
	}

	// Rates
	records := mc.recordsProcessed.Load()
	bytes := mc.bytesProcessed.Load()
	dbWrites := mc.dbWritesCompleted.Load()

	mc.mu.Lock()
	mc.recordsPerSecond = float64(records) / elapsed
	mc.bytesPerSecond = float64(bytes) / elapsed
	mc.dbWritesPerSecond = float64(dbWrites) / elapsed
	cpuUsage := mc.cpuUsagePercent
	memUsage := mc.memoryUsageMB
	goroutines := mc.goroutineCount
	mc.mu.Unlock()

	// Current values
	currentFileProc := mc.currentFileProcessing.Load()
	totalFileProc := mc.totalFileProcessing.Load()
	files := mc.filesProcessed.Load()
	errors := mc.fileProcessingErrors.Load()
	dbErrors := mc.dbWriteErrors.Load()
	totalProcessNanos := mc.totalProcessTime.Load()

	// Averages
	avgFileProcessingMs := float64(0)
	if totalFileProc > 0 {
		avgFileProcessingMs = float64(totalProcessNanos) / float64(totalFileProc) / 1e6
	}

	mc.logging.LogInfo("Connector Performance Metrics",
		"interval_seconds", elapsed,
		"records_processed", records,
		"records_per_second", mc.recordsPerSecond,
		"bytes_processed", bytes,
		"bytes_per_second", mc.bytesPerSecond,
		"mb_per_second", mc.bytesPerSecond/1024/1024,
		"files_processed", files,
		"db_writes", dbWrites,
		"db_writes_per_second", mc.dbWritesPerSecond,
		"current_file_processing", currentFileProc,
		"total_file_processing", totalFileProc,
		"avg_file_processing_ms", avgFileProcessingMs,
		"file_processing_errors", errors,
		"db_write_errors", dbErrors,
		"cpu_usage_percent", cpuUsage,
		"memory_usage_mb", memUsage,
		"goroutines", goroutines,
	)

	// Reset counters for next interval
	mc.recordsProcessed.Store(0)
	mc.bytesProcessed.Store(0)
	mc.filesProcessed.Store(0)
	mc.dbWritesCompleted.Store(0)
	mc.totalFileProcessing.Store(0)
	mc.fileProcessingErrors.Store(0)
	mc.dbWriteErrors.Store(0)
	mc.totalProcessTime.Store(0)
	mc.lastResetTime = time.Now()
}
