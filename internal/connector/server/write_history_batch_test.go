package server

import (
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/surrealdb/fivetran-destination/internal/connector/server/testframework"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// setupWriteHistoryBatchTest creates a temp directory for CSV files and returns cleanup function
func setupWriteHistoryBatchTest(t *testing.T) (string, func()) {
	tempDir := t.TempDir()
	cleanup := func() {
		// Temp dir is automatically cleaned up by t.TempDir()
	}
	return tempDir, cleanup
}

// buildHistoryTable creates a table definition with history mode columns
func buildHistoryTable() *pb.Table {
	return testframework.NewTableDefinition("users", map[string]pb.DataType{
		"_fivetran_id":     pb.DataType_STRING,
		"_fivetran_start":  pb.DataType_UTC_DATETIME,
		"_fivetran_end":    pb.DataType_UTC_DATETIME,
		"_fivetran_active": pb.DataType_BOOLEAN,
		"_fivetran_synced": pb.DataType_UTC_DATETIME,
		"name":             pb.DataType_STRING,
		"age":              pb.DataType_INT,
		"active":           pb.DataType_BOOLEAN,
	}, []string{"_fivetran_id", "_fivetran_start"})
}

// buildHistoryTableWithIdNameAmount creates a table definition with id, name, amount columns
// This tests the edge case where the source data has a primary key column named "id",
// which requires special handling due to SurrealDB's internal "id" field being a Record ID.
func buildHistoryTableWithIdNameAmount() *pb.Table {
	return testframework.NewTableDefinition("orders", map[string]pb.DataType{
		"id":               pb.DataType_STRING,
		"_fivetran_start":  pb.DataType_UTC_DATETIME,
		"_fivetran_end":    pb.DataType_UTC_DATETIME,
		"_fivetran_active": pb.DataType_BOOLEAN,
		"_fivetran_synced": pb.DataType_UTC_DATETIME,
		"name":             pb.DataType_STRING,
		"amount":           pb.DataType_INT,
	}, []string{"id", "_fivetran_start"})
}

// createHistoryRecords returns sample test data for history mode table
// Records include _fivetran_start timestamps for version tracking
func createHistoryRecords(startTime time.Time) ([]string, [][]string) {
	columns := []string{"_fivetran_id", "_fivetran_start", "_fivetran_end", "_fivetran_active", "_fivetran_synced", "name", "age", "active"}

	endTime := "9999-12-31T23:59:59Z"
	syncTime := time.Now().UTC().Format(time.RFC3339)

	records := [][]string{
		{"user1", startTime.Format(time.RFC3339), endTime, "true", syncTime, "Alice", "25", "true"},
		{"user2", startTime.Format(time.RFC3339), endTime, "true", syncTime, "Bob", "30", "false"},
		{"user3", startTime.Format(time.RFC3339), endTime, "true", syncTime, "Charlie", "35", "true"},
	}
	return columns, records
}

// assertHistoryRecordExists verifies a history record with specific version timestamp
func assertHistoryRecordExists(t *testing.T, config map[string]string, namespace, database, tableName, id, startTime string, expectedValues map[string]interface{}) {
	records := testframework.QueryTable(t, config, namespace, database, tableName)

	// Find record matching id and start time
	var found *map[string]interface{}
	for i := range records {
		record := records[i]
		// Compare _fivetran_start
		recordStart := record["_fivetran_start"]
		var recordStartStr string
		switch v := recordStart.(type) {
		case models.CustomDateTime:
			recordStartStr = v.Format(time.RFC3339)
		default:
			t.Fatalf("unexpected type for _fivetran_start %v of %t", recordStart, recordStart)
		}

		if record["_fivetran_id"] == id && recordStartStr == startTime {
			found = &record
			break
		}
	}

	assert.NotNil(t, found, "History record with id=%s and start=%s not found", id, startTime)
}

func assertHistoryRecordValues(t *testing.T, found map[string]interface{}, expectedValues map[string]interface{}) {
	// Verify expected values
	for col, expectedVal := range expectedValues {
		actualVal, exists := found[col]
		assert.True(t, exists, "Column %s not found in record", col)
		assert.Equal(t, expectedVal, actualVal, "Column %s has unexpected value", col)
	}
}

// assertActiveRecord verifies a record is the active version
func assertActiveRecord(t *testing.T, config map[string]string, namespace, database, tableName, id string, startTime models.CustomDateTime) map[string]any {
	records := testframework.QueryTable(t, config, namespace, database, tableName)

	activeCount := 0
	maxEndTime := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

	var found map[string]any

	for i, record := range records {
		t.Logf("Record %d: %+v", i, record)
		ftStart, ok := record["_fivetran_start"]
		require.True(t, ok, "_fivetran_start not found in record %+v", record)
		ftStartCustomDT, ok := ftStart.(models.CustomDateTime)
		require.True(t, ok, "_fivetran_start is not CustomDateTime in record %+v", record)
		if record["_fivetran_id"] == id && ftStartCustomDT.Equal(startTime.Time) && record["_fivetran_active"] == true {
			activeCount++
			// _fivetran_end can be either string or CustomDateTime
			endTime := record["_fivetran_end"]
			switch v := endTime.(type) {
			case models.CustomDateTime:
				assert.True(t, v.Equal(maxEndTime), "Active record should have max end time")
			default:
				t.Fatalf("unexpected type for _fivetran_end %v of %t", endTime, endTime)
			}
			found = record
		}
	}

	require.Equal(t, 1, activeCount, "Should have exactly one active record for id=%s", id)

	return found
}

// assertInactiveRecord verifies a record is an inactive (historical) version
func assertInactiveRecord(t *testing.T, config map[string]string, namespace, database, tableName, id, startTime string) map[string]any {
	records := testframework.QueryTable(t, config, namespace, database, tableName)

	maxEndTime := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

	var inactiveCount int
	var found map[string]any

	for _, record := range records {
		// Compare _fivetran_start as string
		recordStart := record["_fivetran_start"]
		var recordStartStr string
		switch v := recordStart.(type) {
		case models.CustomDateTime:
			recordStartStr = v.Format(time.RFC3339)
		default:
			t.Fatalf("unexpected type for _fivetran_start %v of %T in %+v", recordStart, recordStart, record)
		}

		if record["_fivetran_id"] == id && recordStartStr == startTime {
			inactiveCount++

			assert.Equal(t, false, record["_fivetran_active"], "Record should be inactive")

			// Check that _fivetran_end is NOT the max time
			endTime := record["_fivetran_end"]
			switch v := endTime.(type) {
			case models.CustomDateTime:
				assert.NotEqual(t, maxEndTime, v.Time, "Inactive record should have specific end time")
			default:
				t.Fatalf("unexpected type for _fivetran_end %v of %t", endTime, endTime)
			}

			found = record
		}
	}

	assert.Equal(t, 1, inactiveCount, "Should have exactly one inactive record for id=%s and start=%s", id, startTime)

	return found
}

func TestWriteHistoryBatch_SuccessReplaceSimple(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Create encrypted CSV file with history records
	startTime := time.Now().UTC().Truncate(time.Second)
	columns, records := createHistoryRecords(startTime)
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)

	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "replace_simple.csv", columns, records, key)

	// Execute WriteHistoryBatch
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	// Assert success
	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteHistoryBatch success response")
	require.True(t, success.Success)

	// Verify data in database
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 3)

	ftStart := models.CustomDateTime{Time: startTime}

	// Verify all records are active
	assertActiveRecord(t, config, "test", schema, table.Name, "user1", ftStart)
	assertActiveRecord(t, config, "test", schema, table.Name, "user2", ftStart)
	assertActiveRecord(t, config, "test", schema, table.Name, "user3", ftStart)
}

func TestWriteHistoryBatch_SuccessReplaceAndUpdate(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Insert first version
	startTime1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	endTime := "9999-12-31T23:59:59Z"
	syncTime := time.Now().UTC().Format(time.RFC3339)

	columns := []string{"_fivetran_id", "_fivetran_start", "_fivetran_end", "_fivetran_active", "_fivetran_synced", "name", "age", "active"}
	records1 := [][]string{
		{"user1", startTime1.Format(time.RFC3339), endTime, "true", syncTime, "Alice", "25", "true"},
	}
	key1, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile1 := testframework.CreateEncryptedCSV(t, tempDir, "version1.csv", columns, records1, key1)

	// Insert second version (updated record)
	startTime2 := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)
	records2 := [][]string{
		// This should update the existing row of user1:startTime1 to have end time of startTime2-1ms and fivetran_active=false
		{"user1", startTime2.Format(time.RFC3339), endTime, "true", syncTime, "Alice Updated", "unmodifiedstring56789", "false"},
	}
	key2, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile2 := testframework.CreateEncryptedCSV(t, tempDir, "version2.csv", columns, records2, key2)

	_, err = srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile1},
		UpdateFiles:   []string{csvFile2},
		Keys: map[string][]byte{
			csvFile1: key1,
			csvFile2: key2,
		},
		FileParams: testframework.GetTestFileParams(),
	})
	require.NoError(t, err)

	// Verify both versions exist
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 2)

	// Verify first version is inactive
	assertHistoryRecordValues(t,
		assertInactiveRecord(t, config, "test", schema, table.Name, "user1", startTime1.Format(time.RFC3339)),
		map[string]any{
			"name":   "Alice",
			"age":    uint64(25),
			"active": true,
		})

	// Verify second version is active
	assertHistoryRecordValues(t,
		assertActiveRecord(t, config, "test", schema, table.Name, "user1", models.CustomDateTime{Time: startTime2}),
		map[string]any{
			"name":   "Alice Updated",
			"age":    uint64(25),
			"active": false,
		})
}

func TestWriteHistoryBatch_SuccessReplaceAndDelete(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Insert initial version via replace
	startTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	endTime := "9999-12-31T23:59:59Z"
	syncTime := time.Now().UTC().Format(time.RFC3339)

	columns := []string{"_fivetran_id", "_fivetran_start", "_fivetran_end", "_fivetran_active", "_fivetran_synced", "name", "age", "active"}
	records := [][]string{
		{"user1", startTime.Format(time.RFC3339), endTime, "true", syncTime, "Alice", "25", "true"},
	}
	replaceKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	replaceFile := testframework.CreateEncryptedCSV(t, tempDir, "version1.csv", columns, records, replaceKey)

	// Delete the record
	deleteTime := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)
	deleteRecords := [][]string{
		{"user1", "nullstring01234", deleteTime.Format(time.RFC3339), "false", syncTime, "nullstring01234", "nullstring01234", "nullstring01234"},
	}
	deleteKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	deleteFile := testframework.CreateEncryptedCSV(t, tempDir, "delete.csv", columns, deleteRecords, deleteKey)

	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{replaceFile},
		DeleteFiles:   []string{deleteFile},
		Keys: map[string][]byte{
			replaceFile: replaceKey,
			deleteFile:  deleteKey,
		},
		FileParams: testframework.GetTestFileParams(),
	})
	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteHistoryBatch success response")
	require.True(t, success.Success)

	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 1)

	recordsFound := testframework.QueryTable(t, config, "test", schema, table.Name)
	require.Len(t, recordsFound, 1, "Should have exactly one record after delete")
	record := recordsFound[0]

	require.Equal(t, "user1", record["_fivetran_id"])
	assertHistoryRecordValues(t, record, map[string]any{
		"name":   "Alice",
		"age":    uint64(25),
		"active": true,
	})
	require.Equal(t, false, record["_fivetran_active"], "Record should be marked inactive after delete")

	startVal, ok := record["_fivetran_start"].(models.CustomDateTime)
	require.True(t, ok, "_fivetran_start should be CustomDateTime")
	require.Equal(t, startTime, startVal.Time, "_fivetran_start should remain the original start time")

	endVal, ok := record["_fivetran_end"].(models.CustomDateTime)
	require.True(t, ok, "_fivetran_end should be CustomDateTime")
	require.Equal(t, deleteTime, endVal.Time, "_fivetran_end should be delete time")

	syncedVal, ok := record["_fivetran_synced"].(models.CustomDateTime)
	require.True(t, ok, "_fivetran_synced should be CustomDateTime")
	require.Equal(t, syncTime, syncedVal.Format(time.RFC3339), "_fivetran_synced should match sync time")
}

func TestWriteHistoryBatch_SuccessEarliestStart(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Insert three versions at different times
	endTime := "9999-12-31T23:59:59Z"
	syncTime := time.Now().UTC().Format(time.RFC3339)
	columns := []string{"_fivetran_id", "_fivetran_start", "_fivetran_end", "_fivetran_active", "_fivetran_synced", "name", "age", "active"}

	startTime1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	startTime2 := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)
	startTime3 := time.Date(2024, 1, 3, 12, 0, 0, 0, time.UTC)

	allRecords := [][]string{
		{"user1", startTime1.Format(time.RFC3339), endTime, "false", syncTime, "Alice v1", "25", "true"},
		{"user1", startTime2.Format(time.RFC3339), endTime, "false", syncTime, "Alice v2", "26", "true"},
		{"user1", startTime3.Format(time.RFC3339), endTime, "true", syncTime, "Alice v3", "27", "true"},
	}
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "versions.csv", columns, allRecords, key)

	_, err = srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})
	require.NoError(t, err)

	// Verify all 3 versions exist
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 3)

	// Create earliest_start file to delete versions from T2 onwards
	earliestColumns := []string{"_fivetran_id", "_fivetran_start"}
	earliestRecords := [][]string{
		{"user1", startTime2.Format(time.RFC3339)},
	}
	earliestKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	earliestFile := testframework.CreateEncryptedCSV(t, tempDir, "earliest.csv", earliestColumns, earliestRecords, earliestKey)

	// Execute earliest_start operation
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration:      config,
		SchemaName:         schema,
		Table:              table,
		EarliestStartFiles: []string{earliestFile},
		Keys:               map[string][]byte{earliestFile: earliestKey},
		FileParams:         testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteHistoryBatch success response")
	require.True(t, success.Success)

	// Verify only T1 version remains (T2 and T3 deleted)
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 1)
	assertHistoryRecordExists(t, config, "test", schema, table.Name, "user1", startTime1.Format(time.RFC3339),
		map[string]any{
			"name":             "Alice v1",
			"age":              uint64(25),
			"_fivetran_active": false,
			"_fivetran_end":    models.CustomDateTime{Time: startTime2.Add(-time.Millisecond)},
		})
}

func TestWriteHistoryBatch_SuccessDelete(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Insert initial data
	startTime := time.Now().UTC()
	columns, records := createHistoryRecords(startTime)
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "initial.csv", columns, records, key)

	_, err = srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})
	require.NoError(t, err)

	// Create delete file - mark user2 as deleted
	endTime := time.Now().UTC().Format(time.RFC3339)
	syncTime := time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339)

	deleteColumns := []string{"_fivetran_id", "_fivetran_start", "_fivetran_end", "_fivetran_active", "_fivetran_synced", "name", "age", "active"}
	deleteRecords := [][]string{
		{"user2", "nullstring01234", endTime, "false", syncTime, "nullstring01234", "nullstring01234", "nullstring01234"},
	}
	deleteKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	deleteFile := testframework.CreateEncryptedCSV(t, tempDir, "delete.csv", deleteColumns, deleteRecords, deleteKey)

	// Execute delete
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		DeleteFiles:   []string{deleteFile},
		Keys:          map[string][]byte{deleteFile: deleteKey},
		FileParams:    testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteHistoryBatch success response")
	require.True(t, success.Success)

	// Verify user2 has delete marker record (total 4 records: 2 initial + 1 deleted)
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 3)
}

func TestWriteHistoryBatch_SuccessMixedOperations(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Prepare all operations in a SINGLE batch
	endTime := "9999-12-31T23:59:59Z"
	syncTime := time.Now().UTC().Format(time.RFC3339)
	columns := []string{"_fivetran_id", "_fivetran_start", "_fivetran_end", "_fivetran_active", "_fivetran_synced", "name", "age", "active"}

	startTime1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	startTime3 := time.Date(2024, 1, 3, 12, 0, 0, 0, time.UTC)
	startTime4 := time.Date(2024, 1, 4, 12, 0, 0, 0, time.UTC)
	startTime5 := time.Date(2024, 1, 5, 12, 0, 0, 0, time.UTC)

	// 1. Replace: Create initial state + new records
	// - user1 at T1 (will be updated later in this batch)
	// - user2 at T1 (will be deleted later in this batch)
	// - user3 at T1 and T3 (T3 will be removed by earliest_start)
	// - user4 at T4 (new record)
	replaceRecords := [][]string{
		{"user1", startTime1.Format(time.RFC3339), endTime, "true", syncTime, "Alice v1", "25", "true"},
		{"user2", startTime1.Format(time.RFC3339), endTime, "true", syncTime, "Bob", "30", "false"},
		// This is the first version of user3
		{"user3", startTime1.Format(time.RFC3339), startTime3.Add(-time.Millisecond).Format(time.RFC3339), "false", syncTime, "Charlie v1", "35", "true"},
		// This is the second version of user3
		{"user3", startTime3.Format(time.RFC3339), endTime, "true", syncTime, "Charlie v2", "36", "true"},
		{"user4", startTime4.Format(time.RFC3339), endTime, "true", syncTime, "Diana", "28", "true"},
	}
	replaceKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	replaceFile := testframework.CreateEncryptedCSV(t, tempDir, "replace.csv", columns, replaceRecords, replaceKey)

	// 2. Earliest start: remove user3's versions >= T3 (should delete T3 version, keep T1)
	earliestColumns := []string{"_fivetran_id", "_fivetran_start"}
	earliestRecords := [][]string{
		{"user1", startTime1.Format(time.RFC3339)},
		{"user2", startTime1.Format(time.RFC3339)},
		{"user3", startTime1.Format(time.RFC3339)},
		{"user4", startTime4.Format(time.RFC3339)},
	}
	earliestKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	earliestFile := testframework.CreateEncryptedCSV(t, tempDir, "earliest.csv", earliestColumns, earliestRecords, earliestKey)

	// 3. Update: Update user1 with unmodified field handling
	// Should fetch name and active from user1's T1 version
	updateRecords := [][]string{
		{"user1", startTime4.Format(time.RFC3339), endTime, "true", syncTime, "unmodifiedstring56789", "27", "unmodifiedstring56789"},
	}
	updateKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	updateFile := testframework.CreateEncryptedCSV(t, tempDir, "update.csv", columns, updateRecords, updateKey)

	// 4. Delete: Mark user2's latest version as deleted
	deleteRecords := [][]string{
		{"user2", "nullstring01234", startTime5.Format(time.RFC3339), "false", syncTime, "nullstring01234", "nullstring01234", "nullstring01234"},
	}
	deleteKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	deleteFile := testframework.CreateEncryptedCSV(t, tempDir, "delete.csv", columns, deleteRecords, deleteKey)

	// Execute single batch with all 4 file types
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration:      config,
		SchemaName:         schema,
		Table:              table,
		EarliestStartFiles: []string{earliestFile},
		ReplaceFiles:       []string{replaceFile},
		UpdateFiles:        []string{updateFile},
		DeleteFiles:        []string{deleteFile},
		Keys: map[string][]byte{
			earliestFile: earliestKey,
			replaceFile:  replaceKey,
			updateFile:   updateKey,
			deleteFile:   deleteKey,
		},
		FileParams: testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteHistoryBatch success response")
	require.True(t, success.Success)

	// All the replace and update records should be present as historical records.
	// The delete record should update the latest version of user2 to be marked as deleted (fivetran_active=false and fivetran_start = delete timestamp)
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 6)

	// Query all records for detailed verification
	records := testframework.QueryTable(t, config, "test", schema, table.Name)

	// Verify user1 has 2 versions
	user1Count := 0
	for _, record := range records {
		if record["_fivetran_id"] == "user1" {
			user1Count++
		}
	}
	require.Equal(t, 2, user1Count, "user1 should have 2 versions (T1 and T4)")

	// Verify user1 T4 version has merged values (name from T1, age from update)
	assertHistoryRecordExists(t, config, "test", schema, table.Name, "user1", startTime4.Format(time.RFC3339),
		map[string]interface{}{
			"name":   "Alice v1", // Unmodified - should come from T1
			"age":    uint64(27), // Modified - should come from update file
			"active": true,       // Unmodified - should come from T1
		})

	// Verify user2 is marked as deleted
	user2Found := false
	for _, record := range records {
		if record["_fivetran_id"] == "user2" {
			user2Found = true
			require.False(t, false, record["_fivetran_active"], "user2 should be marked as deleted")

			startTimeVal := record["_fivetran_start"]
			switch v := startTimeVal.(type) {
			case models.CustomDateTime:
				require.Equal(t, startTime1, v.Time, "user2 should have original start time as start time")
			default:
				t.Fatalf("unexpected type for _fivetran_start %v of %T", startTimeVal, startTimeVal)
			}

			// Verify _fivetran_end is the delete timestamp, not max time
			endTimeVal := record["_fivetran_end"]
			switch v := endTimeVal.(type) {
			case models.CustomDateTime:
				require.Equal(t, startTime5, v.Time, "user2 should have delete timestamp as end time")
			default:
				t.Fatalf("unexpected type for _fivetran_end %v of %T", endTimeVal, endTimeVal)
			}
		}
	}
	require.True(t, user2Found, "user2 should exist in database")

	// Verify user3 has 2 versions
	user3Count := 0
	for _, record := range records {
		if record["_fivetran_id"] == "user3" {
			user3Count++
		}
	}
	require.Equal(t, 2, user3Count, "user3 should have 2 versions")

	// Verify user4 exists
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user4"},
		map[string]interface{}{"name": "Diana", "age": uint64(28)})
}

// Failure test cases

func TestWriteHistoryBatch_FailureInvalidConfig(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	table := buildHistoryTable()

	// Create a CSV file
	startTime := time.Now().UTC()
	columns, records := createHistoryRecords(startTime)
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "test.csv", columns, records, key)

	// Use invalid configuration (bad credentials)
	invalidConfig := map[string]string{
		"url":  testframework.GetSurrealDBEndpoint(),
		"ns":   "test",
		"user": "invalid_user",
		"pass": "invalid_password",
	}

	// Execute WriteHistoryBatch with invalid config
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: invalidConfig,
		SchemaName:    "test_writehistorybatch",
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	// Should fail
	require.Error(t, err)
	require.NotNil(t, batchResp)
	warning, ok := batchResp.Response.(*pb.WriteBatchResponse_Warning)
	require.True(t, ok, "Expected WriteHistoryBatch warning response")
	require.NotEmpty(t, warning.Warning)
}

func TestWriteHistoryBatch_FailureTableNotExists(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()

	// Create table definition but don't create it in database
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create CSV file
	startTime := time.Now().UTC()
	columns, records := createHistoryRecords(startTime)
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "test.csv", columns, records, key)

	// Execute WriteHistoryBatch on non-existent table
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	// Should fail
	require.Error(t, err)
	require.NotNil(t, batchResp)
	warning, ok := batchResp.Response.(*pb.WriteBatchResponse_Warning)
	require.True(t, ok, "Expected WriteHistoryBatch warning response")
	require.NotEmpty(t, warning.Warning)
}

func TestWriteHistoryBatch_FailureInvalidEncryptionKey(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Create encrypted CSV file with one key
	startTime := time.Now().UTC()
	columns, records := createHistoryRecords(startTime)
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "test.csv", columns, records, key)

	// Use wrong key for decryption
	wrongKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)

	// Execute WriteHistoryBatch with wrong key
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: wrongKey}, // Wrong key
		FileParams:    testframework.GetTestFileParams(),
	})

	// Should fail
	require.Error(t, err)
	require.NotNil(t, batchResp)
	warning, ok := batchResp.Response.(*pb.WriteBatchResponse_Warning)
	require.True(t, ok, "Expected WriteHistoryBatch warning response")
	require.NotEmpty(t, warning.Warning)
}

func TestWriteHistoryBatch_FailureFileNotFound(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Use non-existent file path
	nonExistentFile := "/tmp/does_not_exist_history_12345.csv"
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)

	// Execute WriteHistoryBatch with non-existent file
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{nonExistentFile},
		Keys:          map[string][]byte{nonExistentFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	// Should fail
	require.Error(t, err)
	require.NotNil(t, batchResp)
	warning, ok := batchResp.Response.(*pb.WriteBatchResponse_Warning)
	require.True(t, ok, "Expected WriteHistoryBatch warning response")
	require.NotEmpty(t, warning.Warning)
}

func TestWriteHistoryBatch_FailureMissingFivetranIdOnlyInCSV(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTable()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Create update CSV without _fivetran_id (invalid for update operation)
	updateTime := time.Now().UTC()
	endTime := "9999-12-31T23:59:59Z"
	syncTime := time.Now().UTC().Format(time.RFC3339)

	// Missing _fivetran_id column
	updateColumns := []string{"_fivetran_start", "_fivetran_end", "_fivetran_active", "_fivetran_synced", "name", "age", "active"}
	updateRecords := [][]string{
		{updateTime.Format(time.RFC3339), endTime, "true", syncTime, "Alice", "25", "true"},
	}
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "no_id.csv", updateColumns, updateRecords, key)

	// Execute WriteHistoryBatch update without _fivetran_id
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		UpdateFiles:   []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	// Should fail
	require.Error(t, err)
	require.NotNil(t, batchResp)
	warning, ok := batchResp.Response.(*pb.WriteBatchResponse_Warning)
	require.True(t, ok, "Expected WriteHistoryBatch warning response")
	require.NotEmpty(t, warning.Warning)
}

func TestWriteHistoryBatch_ReplaceUpdateDelete(t *testing.T) {
	tempDir, cleanup := setupWriteHistoryBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildHistoryTableWithIdNameAmount()
	schema := "test_writehistorybatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Define timestamps
	endTime := "9999-12-31T23:59:59Z"
	syncTime := time.Now().UTC().Format(time.RFC3339)

	startTime1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC) // T1: Initial replace
	startTime2 := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC) // T2: Update time
	deleteTime := time.Date(2024, 1, 3, 12, 0, 0, 0, time.UTC) // T3: Delete synced time

	columns := []string{"id", "_fivetran_start", "_fivetran_end", "_fivetran_active", "_fivetran_synced", "name", "amount"}

	// 1. Replace CSV: Two entries (id1 and id2) at T1
	replaceRecords := [][]string{
		{"id1", startTime1.Format(time.RFC3339), endTime, "true", syncTime, "Alice", "100"},
		{"id2", startTime1.Format(time.RFC3339), endTime, "true", syncTime, "Bob", "200"},
	}
	replaceKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	replaceFile := testframework.CreateEncryptedCSV(t, tempDir, "replace.csv", columns, replaceRecords, replaceKey)

	// 2. Update CSV: Update id1 at T2
	// Uses "unmodifiedstring56789" for fields that should inherit values from previous version
	updateRecords := [][]string{
		{"id1", startTime2.Format(time.RFC3339), endTime, "true", syncTime, "Alice Updated", "unmodifiedstring56789"},
	}
	updateKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	updateFile := testframework.CreateEncryptedCSV(t, tempDir, "update.csv", columns, updateRecords, updateKey)

	// 3. Delete CSV: Delete id2 with synced time at T3
	// Uses "nullstring01234" for null values (as per existing pattern)
	deleteRecords := [][]string{
		{"id2", "nullstring01234", deleteTime.Format(time.RFC3339), "false", deleteTime.Format(time.RFC3339), "nullstring01234", "nullstring01234"},
	}
	deleteKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	deleteFile := testframework.CreateEncryptedCSV(t, tempDir, "delete.csv", columns, deleteRecords, deleteKey)

	// Execute WriteHistoryBatch with all three file types
	batchResp, err := srv.WriteHistoryBatch(t.Context(), &pb.WriteHistoryBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{replaceFile},
		UpdateFiles:   []string{updateFile},
		DeleteFiles:   []string{deleteFile},
		Keys: map[string][]byte{
			replaceFile: replaceKey,
			updateFile:  updateKey,
			deleteFile:  deleteKey,
		},
		FileParams: testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteHistoryBatch success response")
	require.True(t, success.Success)

	// ASSERTIONS

	// 1. Verify total 3 rows
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 3)

	// Query all records for detailed verification
	records := testframework.QueryTable(t, config, "test", schema, table.Name)

	// Helper to extract the "id" column value from a RecordID
	// SurrealDB returns "id" as a RecordID, not the raw column value
	extractIdValue := func(record map[string]any) string {
		idField, ok := record["id"]
		if !ok {
			return ""
		}
		rid, ok := idField.(models.RecordID)
		if !ok {
			return ""
		}
		idArr, ok := rid.ID.([]any)
		if !ok || len(idArr) == 0 {
			return ""
		}
		idStr, ok := idArr[0].(string)
		if !ok {
			return ""
		}
		return idStr
	}

	// Helper to find records by id and optionally _fivetran_start
	findRecord := func(id string, startTime *time.Time) map[string]any {
		for _, record := range records {
			if extractIdValue(record) != id {
				continue
			}
			if startTime == nil {
				return record
			}
			ftStart, ok := record["_fivetran_start"].(models.CustomDateTime)
			if ok && ftStart.Equal(*startTime) {
				return record
			}
		}
		return nil
	}

	maxEndTime := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

	// 2. Verify id1 has 2 versions
	id1Count := 0
	for _, record := range records {
		if extractIdValue(record) == "id1" {
			id1Count++
		}
	}
	require.Equal(t, 2, id1Count, "id1 should have 2 versions")

	// 2a. Verify id1 original version (T1) is inactive with _fivetran_end = T2-1ms
	id1Original := findRecord("id1", &startTime1)
	require.NotNil(t, id1Original, "id1 original version should exist")
	assert.Equal(t, false, id1Original["_fivetran_active"], "id1 original should be inactive")
	assert.Equal(t, "Alice", id1Original["name"])
	assert.Equal(t, uint64(100), id1Original["amount"])
	// Verify _fivetran_end is T2-1ms (when the new version started)
	id1OrigEnd := id1Original["_fivetran_end"].(models.CustomDateTime)
	assert.Equal(t, startTime2.Add(-time.Millisecond), id1OrigEnd.Time, "id1 original _fivetran_end should be T2-1ms")

	// 2b. Verify id1 updated version (T2) is active
	id1Updated := findRecord("id1", &startTime2)
	require.NotNil(t, id1Updated, "id1 updated version should exist")
	assert.Equal(t, true, id1Updated["_fivetran_active"], "id1 updated should be active")
	assert.Equal(t, "Alice Updated", id1Updated["name"])
	assert.Equal(t, uint64(100), id1Updated["amount"]) // Unmodified - inherited from original
	// Verify _fivetran_end is max time (still active)
	id1UpdEnd := id1Updated["_fivetran_end"].(models.CustomDateTime)
	assert.Equal(t, maxEndTime, id1UpdEnd.Time, "id1 updated _fivetran_end should be max time")

	// 3. Verify id2 has exactly 1 row (deleted)
	id2Count := 0
	var id2Record map[string]any
	for _, record := range records {
		if extractIdValue(record) == "id2" {
			id2Count++
			id2Record = record
		}
	}
	require.Equal(t, 1, id2Count, "id2 should have 1 row")

	// 3a. Verify id2 is marked as deleted
	assert.Equal(t, false, id2Record["_fivetran_active"], "id2 should be inactive (deleted)")

	// 3b. Verify id2 _fivetran_end equals delete synced time
	id2End := id2Record["_fivetran_end"].(models.CustomDateTime)
	assert.Equal(t, deleteTime, id2End.Time, "id2 _fivetran_end should equal delete synced time")
}
