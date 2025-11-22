package server

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"

	"github.com/surrealdb/fivetran-destination/internal/connector/server/testframework"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// setupWriteBatchTest creates a temp directory for CSV files and returns cleanup function
func setupWriteBatchTest(t *testing.T) (string, func()) {
	tempDir := t.TempDir()
	cleanup := func() {
		// Temp dir is automatically cleaned up by t.TempDir()
	}
	return tempDir, cleanup
}

// buildUserTable creates a standard user table definition for tests
func buildUserTable() *pb.Table {
	return testframework.NewTableDefinition("users", map[string]pb.DataType{
		"_fivetran_id": pb.DataType_STRING,
		"name":         pb.DataType_STRING,
		"age":          pb.DataType_INT,
		"active":       pb.DataType_BOOLEAN,
	}, []string{"_fivetran_id"})
}

// createTestRecords returns sample test data for user table
func createTestRecords() ([]string, [][]string) {
	columns := []string{"_fivetran_id", "name", "age", "active"}
	records := [][]string{
		{"user1", "Alice", "25", "true"},
		{"user2", "Bob", "30", "false"},
		{"user3", "Charlie", "35", "true"},
	}
	return columns, records
}

func TestWriteBatch_SuccessReplaceSimple(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Create encrypted CSV file
	columns, records := createTestRecords()
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)

	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "replace_simple.csv", columns, records, key)

	// Execute WriteBatch
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
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
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify data in database
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 3)
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user1"},
		map[string]interface{}{"name": "Alice", "age": uint64(25), "active": true})
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user2"},
		map[string]interface{}{"name": "Bob", "age": uint64(30), "active": false})
}

func TestWriteBatch_SuccessReplaceMultiple(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Create first CSV file
	columns1 := []string{"_fivetran_id", "name", "age", "active"}
	records1 := [][]string{
		{"user1", "Alice", "25", "true"},
		{"user2", "Bob", "30", "false"},
	}
	key1, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile1 := testframework.CreateEncryptedCSV(t, tempDir, "replace1.csv", columns1, records1, key1)

	// Create second CSV file
	columns2 := []string{"_fivetran_id", "name", "age", "active"}
	records2 := [][]string{
		{"user3", "Charlie", "35", "true"},
		{"user4", "Diana", "28", "true"},
	}
	key2, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile2 := testframework.CreateEncryptedCSV(t, tempDir, "replace2.csv", columns2, records2, key2)

	// Execute WriteBatch with multiple files
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile1, csvFile2},
		Keys: map[string][]byte{
			csvFile1: key1,
			csvFile2: key2,
		},
		FileParams: testframework.GetTestFileParams(),
	})

	// Assert success
	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify all 4 records are in database
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 4)
}

func TestWriteBatch_SuccessUpdate(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Insert initial data
	columns, records := createTestRecords()
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "initial.csv", columns, records, key)

	_, err = srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})
	require.NoError(t, err)

	// Create update file - update age for user1 and name for user2
	updateColumns := []string{"_fivetran_id", "name", "age", "active"}
	updateRecords := [][]string{
		{"user1", "Alice Updated", "26", "true"},
		{"user2", "Robert", "31", "false"},
	}
	updateKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	updateFile := testframework.CreateEncryptedCSV(t, tempDir, "update.csv", updateColumns, updateRecords, updateKey)

	// Execute update
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		UpdateFiles:   []string{updateFile},
		Keys:          map[string][]byte{updateFile: updateKey},
		FileParams:    testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify updates
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user1"},
		map[string]interface{}{"name": "Alice Updated", "age": uint64(26)})
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user2"},
		map[string]interface{}{"name": "Robert", "age": uint64(31)})
	// user3 should be unchanged
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user3"},
		map[string]interface{}{"name": "Charlie", "age": uint64(35)})
}

func TestWriteBatch_SuccessUpdateWithUnmodified(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Insert initial data
	columns, records := createTestRecords()
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "initial.csv", columns, records, key)

	_, err = srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})
	require.NoError(t, err)

	// Create update file with unmodified_string for some columns
	updateColumns := []string{"_fivetran_id", "name", "age", "active"}
	updateRecords := [][]string{
		// Only update age for user1, keep name and active unchanged
		{"user1", "unmodifiedstring56789", "26", "unmodifiedstring56789"},
	}
	updateKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	updateFile := testframework.CreateEncryptedCSV(t, tempDir, "update_unmod.csv", updateColumns, updateRecords, updateKey)

	// Execute update
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		UpdateFiles:   []string{updateFile},
		Keys:          map[string][]byte{updateFile: updateKey},
		FileParams:    testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify only age was updated, name and active remain unchanged
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user1"},
		map[string]interface{}{"name": "Alice", "age": uint64(26), "active": true})
}

func TestWriteBatch_SuccessDelete(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Insert initial data
	columns, records := createTestRecords()
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "initial.csv", columns, records, key)

	_, err = srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})
	require.NoError(t, err)

	// Create delete file - delete user2
	deleteColumns := []string{"_fivetran_id"}
	deleteRecords := [][]string{
		{"user2"},
	}
	deleteKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	deleteFile := testframework.CreateEncryptedCSV(t, tempDir, "delete.csv", deleteColumns, deleteRecords, deleteKey)

	// Execute delete
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
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
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify user2 is deleted, others remain
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 2)
	testframework.AssertRecordNotExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user2"})
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user1"},
		map[string]interface{}{"name": "Alice"})
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user3"},
		map[string]interface{}{"name": "Charlie"})
}

func TestWriteBatch_SuccessMixedOperations(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Insert initial data
	columns, records := createTestRecords()
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "initial.csv", columns, records, key)

	_, err = srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})
	require.NoError(t, err)

	// Create files for mixed operations
	// Replace: Add user4
	replaceColumns := []string{"_fivetran_id", "name", "age", "active"}
	replaceRecords := [][]string{
		{"user4", "Diana", "28", "true"},
	}
	replaceKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	replaceFile := testframework.CreateEncryptedCSV(t, tempDir, "replace_new.csv", replaceColumns, replaceRecords, replaceKey)

	// Update: Update user1
	updateColumns := []string{"_fivetran_id", "name", "age", "active"}
	updateRecords := [][]string{
		{"user1", "Alice Modified", "27", "false"},
	}
	updateKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	updateFile := testframework.CreateEncryptedCSV(t, tempDir, "update_mixed.csv", updateColumns, updateRecords, updateKey)

	// Delete: Delete user3
	deleteColumns := []string{"_fivetran_id"}
	deleteRecords := [][]string{
		{"user3"},
	}
	deleteKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	deleteFile := testframework.CreateEncryptedCSV(t, tempDir, "delete_mixed.csv", deleteColumns, deleteRecords, deleteKey)

	// Execute mixed operations
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
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
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify results: Should have user1 (updated), user2 (unchanged), user4 (new)
	// user3 should be deleted
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 3)
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user1"},
		map[string]interface{}{"name": "Alice Modified", "age": uint64(27), "active": false})
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user2"},
		map[string]interface{}{"name": "Bob"})
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user4"},
		map[string]interface{}{"name": "Diana"})
	testframework.AssertRecordNotExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user3"})
}

func TestWriteBatch_SuccessCompositePrimaryKey(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	schema := "test_writebatch"

	// Create table with composite primary key
	table := testframework.NewTableDefinition("transactions", map[string]pb.DataType{
		"user_id":        pb.DataType_STRING,
		"transaction_id": pb.DataType_STRING,
		"amount":         pb.DataType_FLOAT,
	}, []string{"user_id", "transaction_id"})

	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Create CSV with composite key data
	columns := []string{"user_id", "transaction_id", "amount"}
	records := [][]string{
		{"user1", "txn1", "100.50"},
		{"user1", "txn2", "200.75"},
		{"user2", "txn1", "150.25"},
	}
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "composite_pk.csv", columns, records, key)

	// Execute WriteBatch
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify records
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 3)
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"user_id": "user1", "transaction_id": "txn1"},
		map[string]interface{}{"amount": float32(100.5)})
}

func TestWriteBatch_SuccessNullValues(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Create CSV with null values using null_string
	columns := []string{"_fivetran_id", "name", "age", "active"}
	records := [][]string{
		{"user1", "Alice", "25", "true"},
		{"user2", "nullstring01234", "nullstring01234", "false"}, // null name and age
	}
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "nulls.csv", columns, records, key)

	// Execute WriteBatch
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify NULL values were properly stored
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 2)
	testframework.AssertRecordExists(t, config, "test", schema, table.Name,
		map[string]interface{}{"_fivetran_id": "user1"},
		map[string]interface{}{"name": "Alice", "age": uint64(25)})

	// Verify user2 has NULL values for name and age
	records2 := testframework.QueryTable(t, config, "test", schema, table.Name)
	var user2Record map[string]interface{}
	for _, record := range records2 {
		if record["_fivetran_id"] == "user2" {
			user2Record = record
			break
		}
	}
	require.NotNil(t, user2Record)
	require.Nil(t, user2Record["name"], "name should be NULL")
	require.Nil(t, user2Record["age"], "age should be NULL")
	require.Equal(t, false, user2Record["active"])
}

// Failure test cases

func TestWriteBatch_FailureInvalidConfig(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	table := buildUserTable()

	// Create a CSV file
	columns, records := createTestRecords()
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

	// Execute WriteBatch with invalid config
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: invalidConfig,
		SchemaName:    "test_writebatch",
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	// Should fail
	require.Error(t, err)
	require.NotNil(t, batchResp)
	warning, ok := batchResp.Response.(*pb.WriteBatchResponse_Warning)
	require.True(t, ok, "Expected WriteBatch warning response")
	require.NotEmpty(t, warning.Warning)
}

func TestWriteBatch_FailureTableNotExists(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()

	// Create table definition but don't create it in database
	table := buildUserTable()
	schema := "test_writebatch"

	// Create CSV file
	columns, records := createTestRecords()
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "test.csv", columns, records, key)

	// Execute WriteBatch on non-existent table
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
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
	require.True(t, ok, "Expected WriteBatch warning response")
	require.NotEmpty(t, warning.Warning)
}

func TestWriteBatch_FailureInvalidEncryptionKey(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Create encrypted CSV file with one key
	columns, records := createTestRecords()
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "test.csv", columns, records, key)

	// Use wrong key for decryption
	wrongKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)

	// Execute WriteBatch with wrong key
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
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
	require.True(t, ok, "Expected WriteBatch warning response")
	require.NotEmpty(t, warning.Warning)
}

func TestWriteBatch_FailureFileNotFound(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Use non-existent file path
	nonExistentFile := "/tmp/does_not_exist_12345.csv"
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)

	// Execute WriteBatch with non-existent file
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
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
	require.True(t, ok, "Expected WriteBatch warning response")
	require.NotEmpty(t, warning.Warning)
}

func TestWriteBatch_SuccessSingleIDPK(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	schema := "test_writebatch"

	// Create table with single primary key named "id"
	// This tests that our connector properly handles the special "id" column
	// and creates array-based IDs like products:[1] instead of products:1
	table := testframework.NewTableDefinition("products", map[string]pb.DataType{
		"id":    pb.DataType_INT,
		"name":  pb.DataType_STRING,
		"price": pb.DataType_FLOAT,
	}, []string{"id"})

	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Test 1: Upsert (replace) with "id" as primary key
	columns := []string{"id", "name", "price"}
	records := [][]string{
		{"1", "Product A", "10.50"},
		{"2", "Product B", "20.75"},
		{"3", "Product C", "15.25"},
	}
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "id_pk_replace.csv", columns, records, key)

	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify records were created with array-based IDs (products:[1], not products:1)
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 3)

	// Connect to DB and verify the actual record IDs are array-based
	ctx := t.Context()
	db, err := testframework.ConnectAndUse(ctx, config["url"], "test", schema, config["user"], config["pass"])
	require.NoError(t, err, "Failed to connect to database")
	defer func() {
		if err := db.Close(ctx); err != nil {
			t.Logf("Failed to close database connection: %v", err)
		}
	}()

	// Query records and verify ID structure
	result, err := surrealdb.Query[[]map[string]interface{}](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, *result)
	dbRecords := (*result)[0].Result
	require.Len(t, dbRecords, 3)

	// Verify record IDs are array-based: products:[1], products:[2], products:[3]
	expectedID1 := models.NewRecordID("products", []any{uint64(1)})
	expectedID2 := models.NewRecordID("products", []any{uint64(2)})
	expectedID3 := models.NewRecordID("products", []any{uint64(3)})

	require.Equal(t, expectedID1, dbRecords[0]["id"], "Record ID should be products:[1] (array-based)")
	require.Equal(t, expectedID2, dbRecords[1]["id"], "Record ID should be products:[2] (array-based)")
	require.Equal(t, expectedID3, dbRecords[2]["id"], "Record ID should be products:[3] (array-based)")

	// Verify field values
	require.Equal(t, "Product A", dbRecords[0]["name"])
	require.Equal(t, float32(10.5), dbRecords[0]["price"])
	require.Equal(t, "Product B", dbRecords[1]["name"])
	require.Equal(t, float32(20.75), dbRecords[1]["price"])
	require.Equal(t, "Product C", dbRecords[2]["name"])
	require.Equal(t, float32(15.25), dbRecords[2]["price"])

	// Test 2: Update with "id" as primary key
	updateColumns := []string{"id", "name", "price"}
	updateRecords := [][]string{
		{"1", "Product A Updated", "11.00"},
		{"3", "Product C Updated", "16.00"},
	}
	updateKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	updateFile := testframework.CreateEncryptedCSV(t, tempDir, "id_pk_update.csv", updateColumns, updateRecords, updateKey)

	batchResp, err = srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		UpdateFiles:   []string{updateFile},
		Keys:          map[string][]byte{updateFile: updateKey},
		FileParams:    testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok = batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify updates worked correctly and IDs are still array-based
	result, err = surrealdb.Query[[]map[string]interface{}](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	dbRecords = (*result)[0].Result
	require.Len(t, dbRecords, 3)

	// IDs should still be array-based after update
	require.Equal(t, expectedID1, dbRecords[0]["id"], "Record ID should remain products:[1] after update")
	require.Equal(t, expectedID2, dbRecords[1]["id"], "Record ID should remain products:[2] after update")
	require.Equal(t, expectedID3, dbRecords[2]["id"], "Record ID should remain products:[3] after update")

	// Verify field values after update
	require.Equal(t, "Product A Updated", dbRecords[0]["name"])
	require.Equal(t, float32(11.0), dbRecords[0]["price"])
	require.Equal(t, "Product B", dbRecords[1]["name"]) // Unchanged
	require.Equal(t, "Product C Updated", dbRecords[2]["name"])
	require.Equal(t, float32(16.0), dbRecords[2]["price"])

	// Test 3: Delete with "id" as primary key
	deleteColumns := []string{"id"}
	deleteRecords := [][]string{
		{"2"},
	}
	deleteKey, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	deleteFile := testframework.CreateEncryptedCSV(t, tempDir, "id_pk_delete.csv", deleteColumns, deleteRecords, deleteKey)

	batchResp, err = srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		DeleteFiles:   []string{deleteFile},
		Keys:          map[string][]byte{deleteFile: deleteKey},
		FileParams:    testframework.GetTestFileParams(),
	})

	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok = batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteBatch success response")
	require.True(t, success.Success)

	// Verify delete worked correctly - should have 2 records remaining
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 2)

	// Verify remaining records still have array-based IDs
	result, err = surrealdb.Query[[]map[string]interface{}](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	dbRecords = (*result)[0].Result
	require.Len(t, dbRecords, 2)

	require.Equal(t, expectedID1, dbRecords[0]["id"], "Record ID should remain products:[1] after delete")
	require.Equal(t, expectedID3, dbRecords[1]["id"], "Record ID should remain products:[3] after delete")

	// Products 1 and 3 should still exist with their updated values
	require.Equal(t, "Product A Updated", dbRecords[0]["name"])
	require.Equal(t, "Product C Updated", dbRecords[1]["name"])
}

func TestWriteBatch_FailureEmptyCSV(t *testing.T) {
	tempDir, cleanup := setupWriteBatchTest(t)
	defer cleanup()

	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	config := testframework.GetSurrealDBConfig()
	table := buildUserTable()
	schema := "test_writebatch"

	// Create table
	_, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
	})
	require.NoError(t, err)
	defer testframework.DropTable(t, config, "test", schema, table.Name)

	// Create CSV file with headers only, no data rows
	columns := []string{"_fivetran_id", "name", "age", "active"}
	emptyRecords := [][]string{} // No records
	key, err := testframework.GenerateAESKey()
	require.NoError(t, err)
	csvFile := testframework.CreateEncryptedCSV(t, tempDir, "empty.csv", columns, emptyRecords, key)

	// Execute WriteBatch with empty CSV
	batchResp, err := srv.WriteBatch(t.Context(), &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schema,
		Table:         table,
		ReplaceFiles:  []string{csvFile},
		Keys:          map[string][]byte{csvFile: key},
		FileParams:    testframework.GetTestFileParams(),
	})

	// Should succeed (empty CSV is valid, just no data to write)
	require.NoError(t, err)
	require.NotNil(t, batchResp)
	success, ok := batchResp.Response.(*pb.WriteBatchResponse_Success)
	require.True(t, ok, "Expected WriteBatch success response for empty CSV")
	require.True(t, success.Success)

	// Verify no records were inserted
	testframework.AssertRecordCount(t, config, "test", schema, table.Name, 0)
}
