package testframework

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

// QueryTable fetches all records from a table for validation
func QueryTable(t *testing.T, config map[string]string, namespace, database, tableName string) []map[string]interface{} {
	ctx := t.Context()
	db, err := ConnectAndUse(ctx, config["url"], namespace, database, config["user"], config["pass"])
	require.NoError(t, err, "Failed to connect to database for query")
	defer db.Close(ctx)

	result, err := surrealdb.Query[[]map[string]interface{}](ctx, db,
		fmt.Sprintf("SELECT * FROM %s;", tableName),
		nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, result, "Query result is nil")
	require.NotEmpty(t, *result, "Query result is empty")

	return (*result)[0].Result
}

// AssertRecordCount verifies the number of records in a table
func AssertRecordCount(t *testing.T, config map[string]string, namespace, database, tableName string, expectedCount int) {
	records := QueryTable(t, config, namespace, database, tableName)
	require.Len(t, records, expectedCount, fmt.Sprintf("Expected %d records in table %s", expectedCount, tableName))
}

// AssertRecordExists verifies that a record exists with the given primary key and expected values
// primaryKey is a map of column names to values that identify the record
// expectedValues is a map of column names to expected values
func AssertRecordExists(t *testing.T, config map[string]string, namespace, database, tableName string,
	primaryKey map[string]interface{}, expectedValues map[string]interface{}) {

	records := QueryTable(t, config, namespace, database, tableName)

	// Find record matching primary key
	var found *map[string]interface{}
	for i := range records {
		record := records[i]
		matches := true
		for pkCol, pkVal := range primaryKey {
			if record[pkCol] != pkVal {
				matches = false
				break
			}
		}
		if matches {
			found = &record
			break
		}
	}

	require.NotNil(t, found, fmt.Sprintf("Record with primary key %v not found in table %s", primaryKey, tableName))

	// Verify expected values
	for col, expectedVal := range expectedValues {
		actualVal, exists := (*found)[col]
		require.True(t, exists, fmt.Sprintf("Column %s not found in record", col))
		require.Equal(t, expectedVal, actualVal, fmt.Sprintf("Column %s has unexpected value", col))
	}
}

// AssertColumnValue verifies a specific column value for a record identified by primary key
func AssertColumnValue(t *testing.T, config map[string]string, namespace, database, tableName string,
	primaryKey map[string]interface{}, columnName string, expectedValue interface{}) {

	AssertRecordExists(t, config, namespace, database, tableName, primaryKey, map[string]interface{}{
		columnName: expectedValue,
	})
}

// AssertRecordNotExists verifies that a record with the given primary key does NOT exist
func AssertRecordNotExists(t *testing.T, config map[string]string, namespace, database, tableName string,
	primaryKey map[string]interface{}) {

	records := QueryTable(t, config, namespace, database, tableName)

	// Ensure no record matches the primary key
	for _, record := range records {
		matches := true
		for pkCol, pkVal := range primaryKey {
			if record[pkCol] != pkVal {
				matches = false
				break
			}
		}
		if matches {
			t.Fatalf("Record with primary key %v found in table %s but expected not to exist", primaryKey, tableName)
		}
	}
}
