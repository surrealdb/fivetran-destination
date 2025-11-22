package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestCopyTableToHistoryMode_FromLive_BasicConversion(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create live mode source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD email ON source TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON source TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert live mode data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:['alice'] SET name = 'Alice', email = 'alice@example.com', _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE source:['bob'] SET name = 'Bob', email = 'bob@example.com', _fivetran_synced = d'2024-01-02T00:00:00Z';
		CREATE source:['charlie'] SET name = 'Charlie', email = 'charlie@example.com', _fivetran_synced = d'2024-01-03T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy to history mode (empty softDeletedColumn means live mode)
	err = migrator.CopyTableToHistoryMode(ctx, namespace, "", "source", "dest", "")
	require.NoError(t, err, "CopyTableToHistoryMode failed")

	// Verify destination table has history mode records
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records in dest")

	// Verify data preserved
	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, "alice@example.com", records[0]["email"])
	assert.Equal(t, "Bob", records[1]["name"])
	assert.Equal(t, "bob@example.com", records[1]["email"])
	assert.Equal(t, "Charlie", records[2]["name"])
	assert.Equal(t, "charlie@example.com", records[2]["email"])

	// Verify history fields are added
	assert.Contains(t, records[0], "_fivetran_start", "Should have _fivetran_start")
	assert.Contains(t, records[0], "_fivetran_end", "Should have _fivetran_end")
	assert.Contains(t, records[0], "_fivetran_active", "Should have _fivetran_active")
	assert.Contains(t, records[0], "_fivetran_synced", "Should still have _fivetran_synced")

	// Verify all records are active (live mode -> all active)
	assert.True(t, records[0]["_fivetran_active"].(bool), "Alice should be active")
	assert.True(t, records[1]["_fivetran_active"].(bool), "Bob should be active")
	assert.True(t, records[2]["_fivetran_active"].(bool), "Charlie should be active")

	// Verify _fivetran_end is far future
	endTime0 := records[0]["_fivetran_end"].(models.CustomDateTime).Time
	assert.Equal(t, 9999, endTime0.Year(), "_fivetran_end should be year 9999")

	// Verify IDs now include _fivetran_start
	id0 := records[0]["id"].(models.RecordID)
	id0Arr := id0.ID.([]any)
	assert.Len(t, id0Arr, 2, "ID should have 2 elements [pk, _fivetran_start]")
	assert.Equal(t, "alice", id0Arr[0])

	id1 := records[1]["id"].(models.RecordID)
	id1Arr := id1.ID.([]any)
	assert.Len(t, id1Arr, 2, "ID should have 2 elements [pk, _fivetran_start]")
	assert.Equal(t, "bob", id1Arr[0])
}

func TestCopyTableToHistoryMode_FromLive_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty live mode source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON source TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Copy empty table to history mode
	err = migrator.CopyTableToHistoryMode(ctx, namespace, "", "source", "dest", "")
	require.NoError(t, err, "CopyTableToHistoryMode on empty table should not fail")

	// Verify dest table is empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Empty(t, records, "Dest table should be empty")

	// Verify schema has history fields
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE dest", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasStart := fields["_fivetran_start"]
	_, hasEnd := fields["_fivetran_end"]
	_, hasActive := fields["_fivetran_active"]
	assert.True(t, hasStart, "_fivetran_start should exist in schema")
	assert.True(t, hasEnd, "_fivetran_end should exist in schema")
	assert.True(t, hasActive, "_fivetran_active should exist in schema")
}

func TestCopyTableToHistoryMode_FromLive_PreservesOtherData(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create live mode source table with multiple columns
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD customer ON source TYPE option<string>;
		DEFINE FIELD total ON source TYPE option<float>;
		DEFINE FIELD status ON source TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON source TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:['order1'] SET customer = 'John', total = 100.50, status = 'completed', _fivetran_synced = d'2024-01-01T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy to history mode
	err = migrator.CopyTableToHistoryMode(ctx, namespace, "", "source", "dest", "")
	require.NoError(t, err, "CopyTableToHistoryMode failed")

	// Verify all data is preserved
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 1)

	record := records[0]
	assert.Equal(t, "John", record["customer"])
	assert.Equal(t, float32(100.50), record["total"])
	assert.Equal(t, "completed", record["status"])
	assert.Contains(t, record, "_fivetran_synced")
	assert.Contains(t, record, "_fivetran_start")
	assert.Contains(t, record, "_fivetran_end")
	assert.Contains(t, record, "_fivetran_active")
	assert.True(t, record["_fivetran_active"].(bool))
}

func TestCopyTableToHistoryMode_FromLive_MultipleRecords(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create live mode source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD price ON source TYPE option<float>;
		DEFINE FIELD _fivetran_synced ON source TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert multiple records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:['prod1'] SET name = 'Product 1', price = 10.0, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE source:['prod2'] SET name = 'Product 2', price = 20.0, _fivetran_synced = d'2024-01-02T00:00:00Z';
		CREATE source:['prod3'] SET name = 'Product 3', price = 30.0, _fivetran_synced = d'2024-01-03T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy to history mode
	err = migrator.CopyTableToHistoryMode(ctx, namespace, "", "source", "dest", "")
	require.NoError(t, err, "CopyTableToHistoryMode failed")

	// Verify all records converted
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records")

	// Verify all records are active with history fields
	for i, record := range records {
		assert.Contains(t, record, "_fivetran_start", "Record %d should have _fivetran_start", i)
		assert.Contains(t, record, "_fivetran_end", "Record %d should have _fivetran_end", i)
		assert.Contains(t, record, "_fivetran_active", "Record %d should have _fivetran_active", i)
		assert.True(t, record["_fivetran_active"].(bool), "Record %d should be active", i)

		// Verify ID now has 2 elements
		id := record["id"].(models.RecordID)
		idArr := id.ID.([]any)
		assert.Len(t, idArr, 2, "Record %d ID should have 2 elements", i)
	}
}
