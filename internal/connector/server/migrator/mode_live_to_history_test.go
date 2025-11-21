package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestModeLiveToHistory_BasicConversion(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create live-mode table with simple array IDs
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD id ON users TYPE array<any>;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD email ON users TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON users TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert live-mode data with simple array IDs [pk]
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:['alice'] SET name = 'Alice', email = 'alice@example.com', _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE users:['bob'] SET name = 'Bob', email = 'bob@example.com', _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to history mode
	err = migrator.ModeLiveToHistory(ctx, namespace, "users")
	require.NoError(t, err, "ModeLiveToHistory failed")

	// Verify records exist with history IDs [pk, _fivetran_start]
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify data preserved
	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, "alice@example.com", records[0]["email"])
	assert.Equal(t, "Bob", records[1]["name"])
	assert.Equal(t, "bob@example.com", records[1]["email"])

	// Verify history fields are added
	assert.Contains(t, records[0], "_fivetran_start", "Should have _fivetran_start")
	assert.Contains(t, records[0], "_fivetran_end", "Should have _fivetran_end")
	assert.Contains(t, records[0], "_fivetran_active", "Should have _fivetran_active")
	assert.Contains(t, records[0], "_fivetran_synced", "Should still have _fivetran_synced")

	// Verify all records are active
	assert.True(t, records[0]["_fivetran_active"].(bool), "Record should be active")
	assert.True(t, records[1]["_fivetran_active"].(bool), "Record should be active")

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

	// Verify schema has history fields
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE users", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasStart := fields["_fivetran_start"]
	_, hasEnd := fields["_fivetran_end"]
	_, hasActive := fields["_fivetran_active"]
	_, hasSynced := fields["_fivetran_synced"]
	assert.True(t, hasStart, "_fivetran_start should exist in schema")
	assert.True(t, hasEnd, "_fivetran_end should exist in schema")
	assert.True(t, hasActive, "_fivetran_active should exist in schema")
	assert.True(t, hasSynced, "_fivetran_synced should still exist in schema")
}

func TestModeLiveToHistory_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty live-mode table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_table SCHEMAFULL;
		DEFINE FIELD id ON empty_table TYPE array<any>;
		DEFINE FIELD name ON empty_table TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON empty_table TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Convert empty table to history mode
	err = migrator.ModeLiveToHistory(ctx, namespace, "empty_table")
	require.NoError(t, err, "ModeLiveToHistory on empty table should not fail")

	// Verify table is still empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM empty_table", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Empty(t, records, "Table should still be empty")

	// Verify schema has history fields
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_table", nil)
	require.NoError(t, err)
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

func TestModeLiveToHistory_PreservesOtherData(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create live-mode table with multiple columns
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE orders SCHEMAFULL;
		DEFINE FIELD id ON orders TYPE array<any>;
		DEFINE FIELD customer ON orders TYPE option<string>;
		DEFINE FIELD total ON orders TYPE option<float>;
		DEFINE FIELD status ON orders TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON orders TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE orders:['order1'] SET customer = 'John', total = 100.50, status = 'completed', _fivetran_synced = d'2024-01-01T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to history mode
	err = migrator.ModeLiveToHistory(ctx, namespace, "orders")
	require.NoError(t, err, "ModeLiveToHistory failed")

	// Verify all non-history data is preserved
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders", nil)
	require.NoError(t, err)
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

func TestModeLiveToHistory_MultipleRecords(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create live-mode table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD id ON products TYPE array<any>;
		DEFINE FIELD name ON products TYPE option<string>;
		DEFINE FIELD price ON products TYPE option<float>;
		DEFINE FIELD _fivetran_synced ON products TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert multiple records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:['prod1'] SET name = 'Product 1', price = 10.0, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE products:['prod2'] SET name = 'Product 2', price = 20.0, _fivetran_synced = d'2024-01-02T00:00:00Z';
		CREATE products:['prod3'] SET name = 'Product 3', price = 30.0, _fivetran_synced = d'2024-01-03T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to history mode
	err = migrator.ModeLiveToHistory(ctx, namespace, "products")
	require.NoError(t, err, "ModeLiveToHistory failed")

	// Verify all records converted
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err)
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
