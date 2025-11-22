package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestBatchCopyRecordsWithNewIDs_SimplifyIDs(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table with composite IDs [pk, timestamp]
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD value ON source TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert test data with composite array IDs
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:['alice', d'2024-01-01T00:00:00Z'] SET name = 'Alice', value = 100;
		CREATE source:['bob', d'2024-01-02T00:00:00Z'] SET name = 'Bob', value = 200;
		CREATE source:['charlie', d'2024-01-03T00:00:00Z'] SET name = 'Charlie', value = 300;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Create destination table
	_, err = surrealdb.Query[any](ctx, db, `
		DEFINE TABLE dest SCHEMAFULL;
		DEFINE FIELD id ON dest TYPE array<any>;
		DEFINE FIELD name ON dest TYPE option<string>;
		DEFINE FIELD value ON dest TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create dest table")

	// Copy records with simplified IDs (remove timestamp component)
	idExpression := "array::slice(record::id(id), 0, 1)"
	err = migrator.BatchCopyRecordsWithNewIDs(ctx, "source", "*", "dest", idExpression, "*", 1000)
	require.NoError(t, err, "BatchCopyRecordsWithNewIDs failed")

	// Verify all records were copied with simplified IDs
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records in dest")

	// Verify data
	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, uint64(100), records[0]["value"])
	assert.Equal(t, "Bob", records[1]["name"])
	assert.Equal(t, uint64(200), records[1]["value"])
	assert.Equal(t, "Charlie", records[2]["name"])
	assert.Equal(t, uint64(300), records[2]["value"])

	// Verify IDs are simplified (only 1 element)
	id0 := records[0]["id"].(models.RecordID)
	id0Arr := id0.ID.([]any)
	assert.Len(t, id0Arr, 1, "ID should have 1 element")
	assert.Equal(t, "alice", id0Arr[0])

	id1 := records[1]["id"].(models.RecordID)
	id1Arr := id1.ID.([]any)
	assert.Len(t, id1Arr, 1, "ID should have 1 element")
	assert.Equal(t, "bob", id1Arr[0])
}

func TestBatchCopyRecordsWithNewIDs_WithFieldOmit(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD secret ON source TYPE option<string>;
		DEFINE FIELD timestamp ON source TYPE option<datetime>;
		DEFINE FIELD value ON source TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert test data with composite IDs
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:['alice', d'2024-01-01T00:00:00Z'] SET name = 'Alice', secret = 'password123', timestamp = d'2024-01-01T00:00:00Z', value = 100;
		CREATE source:['bob', d'2024-01-02T00:00:00Z'] SET name = 'Bob', secret = 'qwerty', timestamp = d'2024-01-02T00:00:00Z', value = 200;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Create destination table (without secret and timestamp fields)
	_, err = surrealdb.Query[any](ctx, db, `
		DEFINE TABLE dest SCHEMAFULL;
		DEFINE FIELD id ON dest TYPE array<any>;
		DEFINE FIELD name ON dest TYPE option<string>;
		DEFINE FIELD value ON dest TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create dest table")

	// Copy records, omitting secret and timestamp, and simplifying IDs
	idExpression := "array::slice(record::id(id), 0, 1)"
	insertedFields := "* OMIT secret, timestamp"
	err = migrator.BatchCopyRecordsWithNewIDs(ctx, "source", "*", "dest", idExpression, insertedFields, 1000)
	require.NoError(t, err, "BatchCopyRecordsWithNewIDs failed")

	// Verify records were copied without secret and timestamp fields
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records in dest")

	// Verify secret and timestamp fields are not present
	assert.Equal(t, "Alice", records[0]["name"])
	assert.NotContains(t, records[0], "secret", "secret field should be omitted")
	assert.NotContains(t, records[0], "timestamp", "timestamp field should be omitted")
	assert.Equal(t, uint64(100), records[0]["value"])
}

func TestBatchCopyRecordsWithNewIDs_SmallBatchSize(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD value ON source TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert 10 records with composite IDs
	for i := 1; i <= 10; i++ {
		_, err = surrealdb.Query[any](ctx, db, "CREATE type::thing('source', [$id, d'2024-01-01T00:00:00Z']) SET value = $value", map[string]any{
			"id":    i,
			"value": i * 10,
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Create destination table
	_, err = surrealdb.Query[any](ctx, db, `
		DEFINE TABLE dest SCHEMAFULL;
		DEFINE FIELD id ON dest TYPE array<any>;
		DEFINE FIELD value ON dest TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create dest table")

	// Copy with small batch size (2 records per batch) and simplified IDs
	idExpression := "array::slice(record::id(id), 0, 1)"
	err = migrator.BatchCopyRecordsWithNewIDs(ctx, "source", "*", "dest", idExpression, "*", 2)
	require.NoError(t, err, "BatchCopyRecordsWithNewIDs failed")

	// Verify all 10 records were copied
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 10, "Expected 10 records in dest")

	// Verify all values
	for i := range 10 {
		assert.Equal(t, uint64((i+1)*10), records[i]["value"], "Record %d has wrong value", i)
		// Verify IDs are simplified
		id := records[i]["id"].(models.RecordID)
		idArr := id.ID.([]any)
		assert.Len(t, idArr, 1, "ID should have 1 element")
	}
}

func TestBatchCopyRecordsWithNewIDs_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Create destination table
	_, err = surrealdb.Query[any](ctx, db, `
		DEFINE TABLE dest SCHEMAFULL;
		DEFINE FIELD id ON dest TYPE array<any>;
		DEFINE FIELD name ON dest TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create dest table")

	// Copy from empty table
	idExpression := "array::slice(record::id(id), 0, 1)"
	err = migrator.BatchCopyRecordsWithNewIDs(ctx, "source", "*", "dest", idExpression, "*", 1000)
	require.NoError(t, err, "BatchCopyRecordsWithNewIDs on empty table should not fail")

	// Verify dest table is still empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Empty(t, records, "Dest table should be empty")
}

func TestBatchCopyRecordsWithNewIDs_ExtendIDs(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create source table with simple IDs
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD timestamp ON source TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert test data with simple array IDs
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:['alice'] SET name = 'Alice', timestamp = d'2024-01-01T00:00:00Z';
		CREATE source:['bob'] SET name = 'Bob', timestamp = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Create destination table with composite IDs
	_, err = surrealdb.Query[any](ctx, db, `
		DEFINE TABLE dest SCHEMAFULL;
		DEFINE FIELD id ON dest TYPE array<any>;
		DEFINE FIELD name ON dest TYPE option<string>;
		DEFINE FIELD timestamp ON dest TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create dest table")

	// Copy records with extended IDs (add timestamp to ID)
	idExpression := "array::add(record::id(id), timestamp)"
	err = migrator.BatchCopyRecordsWithNewIDs(ctx, "source", "*", "dest", idExpression, "*", 1000)
	require.NoError(t, err, "BatchCopyRecordsWithNewIDs failed")

	// Verify all records were copied with extended IDs
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records in dest")

	// Verify data
	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, "Bob", records[1]["name"])

	// Verify IDs are extended (2 elements: pk + timestamp)
	id0 := records[0]["id"].(models.RecordID)
	id0Arr := id0.ID.([]any)
	assert.Len(t, id0Arr, 2, "ID should have 2 elements")
	assert.Equal(t, "alice", id0Arr[0])

	id1 := records[1]["id"].(models.RecordID)
	id1Arr := id1.ID.([]any)
	assert.Len(t, id1Arr, 2, "ID should have 2 elements")
	assert.Equal(t, "bob", id1Arr[0])
}
