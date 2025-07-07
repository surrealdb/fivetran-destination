package connector

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// memoryTestFunc is a function that performs operations to be tested for memory usage.
// It receives the iteration index as a parameter.
type memoryTestFunc func(i int) error

// runMemoryTest is a helper function that runs a memory test with the given operation function.
// It handles all the memory measurement setup, execution, and cleanup.
func runMemoryTest(t *testing.T, testName string, iterations int, f memoryTestFunc) (runtime.MemStats, runtime.MemStats) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	// Force garbage collection and get baseline
	runtime.GC()
	runtime.GC() // Call twice to ensure cleanup
	time.Sleep(100 * time.Millisecond)

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Run the test operations
	for i := 0; i < iterations; i++ {
		if err := f(i); err != nil {
			t.Logf("%s error (may be expected): %v", testName, err)
		}
	}

	// Force garbage collection and measure again
	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.ReadMemStats(&m2)

	return m1, m2
}

// assertMemoryUsage is a helper function that logs memory statistics and asserts reasonable usage.
func assertMemoryUsage(t *testing.T, testName string, m1, m2 runtime.MemStats, heapThresholdKB int64) {
	// Calculate memory difference
	allocDiff := int64(m2.TotalAlloc - m1.TotalAlloc)
	heapDiff := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)

	t.Logf("%s Memory Stats:", testName)
	t.Logf("  Total allocations difference: %d bytes", allocDiff)
	t.Logf("  Heap usage difference: %d bytes", heapDiff)
	t.Logf("  Number of GC cycles: %d", m2.NumGC-m1.NumGC)
	t.Logf("  Current goroutines: %d", runtime.NumGoroutine())

	// Assert reasonable memory usage
	assert.Less(t, heapDiff, heapThresholdKB*1024,
		"%s heap usage increased more than %dKB", testName, heapThresholdKB)
}

// TestCombinedMemoryUsage runs a basic memory usage test with various operation
func TestCombinedMemoryUsage(t *testing.T) {
	logger, err := LoggerFromEnv()
	assert.NoError(t, err)

	server := NewServer(logger)
	ctx := context.Background()
	testConfig := getSurrealDBConfig()

	m1, m2 := runMemoryTest(t, "Combined", 50, func(i int) error {
		// Lightweight operations
		_, err := server.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
		if err != nil {
			return err
		}

		_, err = server.Capabilities(ctx, &pb.CapabilitiesRequest{})
		if err != nil {
			return err
		}

		// SurrealDB operations (every 10 iterations to avoid overwhelming)
		if i%10 == 0 {
			_, err = server.Test(ctx, &pb.TestRequest{
				Name:          "memory-baseline",
				Configuration: testConfig,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	assertMemoryUsage(t, "Combined", m1, m2, 100)
}

// TestDescribeTableMemoryUsage tests memory usage during table operations
func TestDescribeTableMemoryUsage(t *testing.T) {
	logger, err := LoggerFromEnv()
	assert.NoError(t, err)

	server := NewServer(logger)
	ctx := context.Background()
	testConfig := getSurrealDBConfig()

	m1, m2 := runMemoryTest(t, "DescribeTable", 5, func(i int) error {
		tableName := fmt.Sprintf("test_table_%d", i)
		_, err := server.DescribeTable(ctx, &pb.DescribeTableRequest{
			SchemaName:    "test",
			TableName:     tableName,
			Configuration: testConfig,
		})
		// Note: DescribeTable might return not found, that's ok for memory testing
		return err
	})

	assertMemoryUsage(t, "DescribeTable", m1, m2, 100)
}

// TestCreateTableMemoryUsage tests memory usage with CreateTable operations
func TestCreateTableMemoryUsage(t *testing.T) {
	logger, err := LoggerFromEnv()
	assert.NoError(t, err)

	server := NewServer(logger)
	ctx := context.Background()
	testConfig := getSurrealDBConfig()
	testTable := getTestTable()

	m1, m2 := runMemoryTest(t, "CreateTable", 10, func(i int) error {
		tableName := fmt.Sprintf("memory_test_create_%d", i)
		testTable.Name = tableName

		_, err := server.CreateTable(ctx, &pb.CreateTableRequest{
			Configuration: testConfig,
			SchemaName:    "test",
			Table:         testTable,
		})
		return err
	})

	assertMemoryUsage(t, "CreateTable", m1, m2, 100)
}

// TestAlterTableMemoryUsage tests memory usage with AlterTable operations
func TestAlterTableMemoryUsage(t *testing.T) {
	logger, err := LoggerFromEnv()
	assert.NoError(t, err)

	server := NewServer(logger)
	ctx := context.Background()
	testConfig := getSurrealDBConfig()
	testTable := getTestTable()

	m1, m2 := runMemoryTest(t, "AlterTable", 10, func(i int) error {
		tableName := fmt.Sprintf("memory_test_alter_%d", i)
		testTable.Name = tableName

		_, err := server.AlterTable(ctx, &pb.AlterTableRequest{
			Configuration: testConfig,
			SchemaName:    "test",
			Table:         testTable,
		})
		return err
	})

	assertMemoryUsage(t, "AlterTable", m1, m2, 100)
}

// TestTruncateMemoryUsage tests memory usage with Truncate operations
func TestTruncateMemoryUsage(t *testing.T) {
	logger, err := LoggerFromEnv()
	assert.NoError(t, err)

	server := NewServer(logger)
	ctx := context.Background()
	testConfig := getSurrealDBConfig()

	m1, m2 := runMemoryTest(t, "Truncate", 10, func(i int) error {
		tableName := fmt.Sprintf("memory_test_truncate_%d", i)

		// Test both hard and soft truncate
		if i%2 == 0 {
			// Hard truncate
			_, err := server.Truncate(ctx, &pb.TruncateRequest{
				Configuration: testConfig,
				SchemaName:    "test",
				TableName:     tableName,
				SyncedColumn:  "_fivetran_synced",
			})
			return err
		} else {
			// Soft truncate
			_, err := server.Truncate(ctx, &pb.TruncateRequest{
				Configuration: testConfig,
				SchemaName:    "test",
				TableName:     tableName,
				SyncedColumn:  "_fivetran_synced",
				Soft: &pb.SoftTruncate{
					DeletedColumn: "_fivetran_deleted",
				},
			})
			return err
		}
	})

	assertMemoryUsage(t, "Truncate", m1, m2, 100)
}

// TestWriteBatchMemoryUsage tests memory usage with WriteBatch operations
func TestWriteBatchMemoryUsage(t *testing.T) {
	logger, err := LoggerFromEnv()
	assert.NoError(t, err)

	server := NewServer(logger)
	ctx := context.Background()
	testConfig := getSurrealDBConfig()
	testTable := getTestTable()
	fileParams := getTestFileParams()
	testTable.Name = "memory_test_batch"

	m1, m2 := runMemoryTest(t, "WriteBatch", 5, func(i int) error {
		// Test different batch operation types
		switch i % 3 {
		case 0:
			_, err := server.WriteBatch(ctx, &pb.WriteBatchRequest{
				Configuration: testConfig,
				SchemaName:    "test",
				Table:         testTable,
				ReplaceFiles:  []string{"test_replace_file.csv"},
				FileParams:    fileParams,
			})
			return err
		case 1:
			_, err := server.WriteBatch(ctx, &pb.WriteBatchRequest{
				Configuration: testConfig,
				SchemaName:    "test",
				Table:         testTable,
				UpdateFiles:   []string{"test_update_file.csv"},
				FileParams:    fileParams,
			})
			return err
		case 2:
			_, err := server.WriteBatch(ctx, &pb.WriteBatchRequest{
				Configuration: testConfig,
				SchemaName:    "test",
				Table:         testTable,
				DeleteFiles:   []string{"test_delete_file.csv"},
				FileParams:    fileParams,
			})
			return err
		}
		return nil
	})

	assertMemoryUsage(t, "WriteBatch", m1, m2, 100)
}

// TestWriteHistoryBatchMemoryUsage tests memory usage with WriteHistoryBatch operations
func TestWriteHistoryBatchMemoryUsage(t *testing.T) {
	logger, err := LoggerFromEnv()
	assert.NoError(t, err)

	server := NewServer(logger)
	ctx := context.Background()
	testConfig := getSurrealDBConfig()
	testTable := getTestTable()
	fileParams := getTestFileParams()
	testTable.Name = "memory_test_history"

	m1, m2 := runMemoryTest(t, "WriteHistoryBatch", 20, func(i int) error {
		// Test different history batch operation types
		switch i % 4 {
		case 0:
			_, err := server.WriteHistoryBatch(ctx, &pb.WriteHistoryBatchRequest{
				Configuration:      testConfig,
				SchemaName:         "test",
				Table:              testTable,
				EarliestStartFiles: []string{"test_earliest_start_file.csv"},
				FileParams:         fileParams,
			})
			return err
		case 1:
			_, err := server.WriteHistoryBatch(ctx, &pb.WriteHistoryBatchRequest{
				Configuration: testConfig,
				SchemaName:    "test",
				Table:         testTable,
				ReplaceFiles:  []string{"test_history_replace_file.csv"},
				FileParams:    fileParams,
			})
			return err
		case 2:
			_, err := server.WriteHistoryBatch(ctx, &pb.WriteHistoryBatchRequest{
				Configuration: testConfig,
				SchemaName:    "test",
				Table:         testTable,
				UpdateFiles:   []string{"test_history_update_file.csv"},
				FileParams:    fileParams,
			})
			return err
		case 3:
			_, err := server.WriteHistoryBatch(ctx, &pb.WriteHistoryBatchRequest{
				Configuration: testConfig,
				SchemaName:    "test",
				Table:         testTable,
				DeleteFiles:   []string{"test_history_delete_file.csv"},
				FileParams:    fileParams,
			})
			return err
		}
		return nil
	})

	assertMemoryUsage(t, "WriteHistoryBatch", m1, m2, 120)
}

// TestCapabilitiesMemoryUsage tests memory usage with Capabilities operations
func TestCapabilitiesMemoryUsage(t *testing.T) {
	logger, err := LoggerFromEnv()
	assert.NoError(t, err)

	server := NewServer(logger)
	ctx := context.Background()

	m1, m2 := runMemoryTest(t, "Capabilities", 20, func(i int) error {
		_, err := server.Capabilities(ctx, &pb.CapabilitiesRequest{})
		return err
	})

	// Capabilities should be very lightweight because
	// it only returns static information about the server
	// and does not involve database connections.
	assertMemoryUsage(t, "Capabilities", m1, m2, 5)
}

// TestTestMemoryUsage tests memory usage with Test operations
func TestTestMemoryUsage(t *testing.T) {
	logger, err := LoggerFromEnv()
	assert.NoError(t, err)

	server := NewServer(logger)
	ctx := context.Background()
	testConfig := getSurrealDBConfig()

	m1, m2 := runMemoryTest(t, "Test", 20, func(i int) error {
		_, err := server.Test(ctx, &pb.TestRequest{
			Name:          fmt.Sprintf("memory-test-%d", i),
			Configuration: testConfig,
		})
		return err
	})

	// Test operations involve database connections, so allow more memory
	assertMemoryUsage(t, "Test", m1, m2, 300)
}

// Helper function to create a test table structure
func getTestTable() *pb.Table {
	return &pb.Table{
		Name: "memory_test_table",
		Columns: []*pb.Column{
			{
				Name:       "id",
				Type:       pb.DataType_INT,
				PrimaryKey: true,
			},
			{
				Name:       "name",
				Type:       pb.DataType_STRING,
				PrimaryKey: false,
			},
			{
				Name:       "email",
				Type:       pb.DataType_STRING,
				PrimaryKey: false,
			},
			{
				Name:       "created_at",
				Type:       pb.DataType_UTC_DATETIME,
				PrimaryKey: false,
			},
			{
				Name:       "is_active",
				Type:       pb.DataType_BOOLEAN,
				PrimaryKey: false,
			},
			{
				Name:       "balance",
				Type:       pb.DataType_DECIMAL,
				PrimaryKey: false,
				Params: &pb.DataTypeParams{
					Params: &pb.DataTypeParams_Decimal{
						Decimal: &pb.DecimalParams{
							Precision: 10,
							Scale:     2,
						},
					},
				},
			},
		},
	}
}

// Helper function to create test file parameters
func getTestFileParams() *pb.FileParams {
	return &pb.FileParams{
		Compression:      pb.Compression_OFF,
		Encryption:       pb.Encryption_NONE,
		NullString:       "nullstring01234",
		UnmodifiedString: "unmodifiedstring56789",
	}
}
