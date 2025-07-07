package connector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsCollector(t *testing.T) {
	mockLogger := NewMockLogging()

	mc := NewMetricsCollector(mockLogger, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc.Start(ctx)

	// Simulate some file_procesing
	mc.FileProcessingStarted()
	mc.RecordProcessed(100, 1024)
	mc.FileProcessed()
	mc.DBWriteCompleted(50)
	mc.FileProcessingCompleted(10 * time.Millisecond)

	// Wait for at least one logging interval
	time.Sleep(150 * time.Millisecond)

	// Find the performance metrics log message
	perfMsg := mockLogger.FindMessage("Connector Performance Metrics")
	require.NotNil(t, perfMsg, "Should have logged performance metrics")

	// Verify the logged metrics
	assert.Equal(t, "INFO", perfMsg.Level)
	assert.NotNil(t, perfMsg.Fields["records_processed"])
	assert.Equal(t, int64(100), perfMsg.Fields["records_processed"])
	assert.NotNil(t, perfMsg.Fields["bytes_processed"])
	assert.Equal(t, int64(1024), perfMsg.Fields["bytes_processed"])
	assert.NotNil(t, perfMsg.Fields["files_processed"])
	assert.Equal(t, int64(1), perfMsg.Fields["files_processed"])
	assert.NotNil(t, perfMsg.Fields["db_writes"])
	assert.Equal(t, int64(50), perfMsg.Fields["db_writes"])
	assert.NotNil(t, perfMsg.Fields["records_per_second"])
	assert.Greater(t, perfMsg.Fields["records_per_second"].(float64), 0.0)
	assert.NotNil(t, perfMsg.Fields["bytes_per_second"])
	assert.Greater(t, perfMsg.Fields["bytes_per_second"].(float64), 0.0)
	assert.NotNil(t, perfMsg.Fields["memory_usage_mb"])
	assert.NotNil(t, perfMsg.Fields["goroutines"])

	// Test error counters
	mc.FileProcessingError()
	mc.DBWriteError()

	assert.Equal(t, int64(1), mc.fileProcessingErrors.Load())
	assert.Equal(t, int64(1), mc.dbWriteErrors.Load())

	// Wait for another interval to see error counts
	mockLogger.Clear()
	time.Sleep(150 * time.Millisecond)

	perfMsg = mockLogger.FindMessage("Connector Performance Metrics")
	require.NotNil(t, perfMsg, "Should have logged performance metrics again")
	assert.Equal(t, int64(1), perfMsg.Fields["file_processing_errors"])
	assert.Equal(t, int64(1), perfMsg.Fields["db_write_errors"])
}

func TestMetricsInServer(t *testing.T) {
	mockLogger := NewMockLogging()

	mc := NewMetricsCollector(mockLogger, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc.Start(ctx)

	// Simulate some metrics
	mc.RecordProcessed(10, 100)
	mc.DBWriteCompleted(5)

	// Wait for metrics to be logged
	time.Sleep(150 * time.Millisecond)

	// Verify metrics were logged
	perfMsg := mockLogger.FindMessage("Connector Performance Metrics")
	require.NotNil(t, perfMsg, "Should have logged performance metrics")
	assert.Equal(t, int64(10), perfMsg.Fields["records_processed"])
	assert.Equal(t, int64(100), perfMsg.Fields["bytes_processed"])
	assert.Equal(t, int64(5), perfMsg.Fields["db_writes"])
}

func TestMetricsCollectorWithFileProcessing(t *testing.T) {
	mockLogger := NewMockLogging()
	mc := NewMetricsCollector(mockLogger, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc.Start(ctx)

	// Simulate multiple file processing
	for i := 0; i < 5; i++ {
		mc.FileProcessingStarted()
		time.Sleep(5 * time.Millisecond)
		mc.RecordProcessed(20, 200)
		mc.DBWriteCompleted(20)
		mc.FileProcessingCompleted(5 * time.Millisecond)
	}

	// Wait for metrics log
	time.Sleep(100 * time.Millisecond)

	perfMsg := mockLogger.FindMessage("Connector Performance Metrics")
	require.NotNil(t, perfMsg, "Should have logged performance metrics")

	// Verify aggregated metrics
	assert.Equal(t, int64(100), perfMsg.Fields["records_processed"]) // 5 * 20
	assert.Equal(t, int64(1000), perfMsg.Fields["bytes_processed"])  // 5 * 200
	assert.Equal(t, int64(100), perfMsg.Fields["db_writes"])         // 5 * 20
	assert.Equal(t, int64(5), perfMsg.Fields["total_file_processing"])
	assert.Equal(t, int32(0), perfMsg.Fields["current_file_processing"])

	// Check average operation time is reasonable (around 5ms)
	avgOpTime := perfMsg.Fields["avg_file_processing_ms"].(float64)
	assert.Greater(t, avgOpTime, 4.0)
	assert.Less(t, avgOpTime, 10.0)
}
