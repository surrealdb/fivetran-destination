package migrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestModeHistoryToLive_BasicConversion(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based IDs
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD id ON users TYPE array<any>;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD email ON users TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON users TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON users TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON users TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON users TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert history-mode data with array-based IDs [pk, _fivetran_start]
	startTime1 := models.CustomDateTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
	startTime2 := models.CustomDateTime{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}
	endTimeMax := models.CustomDateTime{Time: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)}

	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:['alice', d'2024-01-01T00:00:00Z'] SET name = 'Alice', email = 'alice@example.com', _fivetran_synced = $synced1, _fivetran_start = $start1, _fivetran_end = $end_max, _fivetran_active = true;
		CREATE users:['bob', d'2024-01-02T00:00:00Z'] SET name = 'Bob', email = 'bob@example.com', _fivetran_synced = $synced2, _fivetran_start = $start2, _fivetran_end = $end_max, _fivetran_active = true;
	`, map[string]any{
		"synced1": startTime1,
		"synced2": startTime2,
		"start1":  startTime1,
		"start2":  startTime2,
		"end_max": endTimeMax,
	})
	require.NoError(t, err, "Failed to insert data")

	// Convert to live mode (keep deleted rows = false, but all are active)
	err = migrator.ModeHistoryToLive(ctx, namespace, "users", false)
	require.NoError(t, err, "ModeHistoryToLive failed")

	// Verify records exist with simplified IDs (no _fivetran_start component)
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

	// Verify history fields are removed
	assert.NotContains(t, records[0], "_fivetran_start", "Should not have _fivetran_start")
	assert.NotContains(t, records[0], "_fivetran_end", "Should not have _fivetran_end")
	assert.NotContains(t, records[0], "_fivetran_active", "Should not have _fivetran_active")
	assert.Contains(t, records[0], "_fivetran_synced", "Should still have _fivetran_synced")

	// Verify IDs are now simple arrays without _fivetran_start
	id0 := records[0]["id"].(models.RecordID)
	assert.Len(t, id0.ID, 1, "ID should have 1 element (pk only)")
	id0Arr := id0.ID.([]any)
	assert.Equal(t, "alice", id0Arr[0])

	id1 := records[1]["id"].(models.RecordID)
	assert.Len(t, id1.ID, 1, "ID should have 1 element (pk only)")
	id1Arr := id1.ID.([]any)
	assert.Equal(t, "bob", id1Arr[0])

	// Verify schema no longer has history fields
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
	assert.False(t, hasStart, "_fivetran_start should not exist in schema")
	assert.False(t, hasEnd, "_fivetran_end should not exist in schema")
	assert.False(t, hasActive, "_fivetran_active should not exist in schema")
	assert.True(t, hasSynced, "_fivetran_synced should still exist in schema")
}

func TestModeHistoryToLive_DeleteInactiveRecords(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD id ON products TYPE array<any>;
		DEFINE FIELD name ON products TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON products TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON products TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON products TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON products TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert mix of active and inactive records
	startTime := models.CustomDateTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
	endTimeMax := models.CustomDateTime{Time: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)}
	endTimeDeleted := models.CustomDateTime{Time: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)}

	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:['prod1', d'2024-01-01T00:00:00Z'] SET name = 'Active Product', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = $end_max, _fivetran_active = true;
		CREATE products:['prod2', d'2024-01-01T00:00:00Z'] SET name = 'Deleted Product', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = $end_deleted, _fivetran_active = false;
		CREATE products:['prod3', d'2024-01-01T00:00:00Z'] SET name = 'Another Active', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = $end_max, _fivetran_active = true;
	`, map[string]any{
		"synced":      startTime,
		"start":       startTime,
		"end_max":     endTimeMax,
		"end_deleted": endTimeDeleted,
	})
	require.NoError(t, err, "Failed to insert data")

	// Convert to live mode with keepDeletedRows = false
	err = migrator.ModeHistoryToLive(ctx, namespace, "products", false)
	require.NoError(t, err, "ModeHistoryToLive failed")

	// Verify only active records remain
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records (inactive deleted)")

	// Verify correct records remain
	names := []string{records[0]["name"].(string), records[1]["name"].(string)}
	assert.Contains(t, names, "Active Product")
	assert.Contains(t, names, "Another Active")
	assert.NotContains(t, names, "Deleted Product")
}

func TestModeHistoryToLive_KeepDeletedRows(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD id ON items TYPE array<any>;
		DEFINE FIELD name ON items TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON items TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON items TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON items TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON items TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert mix of active and inactive records
	startTime := models.CustomDateTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
	endTimeMax := models.CustomDateTime{Time: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)}
	endTimeDeleted := models.CustomDateTime{Time: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)}

	_, err = surrealdb.Query[any](ctx, db, `
		CREATE items:['item1', d'2024-01-01T00:00:00Z'] SET name = 'Active Item', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = $end_max, _fivetran_active = true;
		CREATE items:['item2', d'2024-01-01T00:00:00Z'] SET name = 'Deleted Item', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = $end_deleted, _fivetran_active = false;
	`, map[string]any{
		"synced":      startTime,
		"start":       startTime,
		"end_max":     endTimeMax,
		"end_deleted": endTimeDeleted,
	})
	require.NoError(t, err, "Failed to insert data")

	// Convert to live mode with keepDeletedRows = true
	err = migrator.ModeHistoryToLive(ctx, namespace, "items", true)
	require.NoError(t, err, "ModeHistoryToLive failed")

	// Verify all records remain (including inactive)
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records (all kept)")

	// Verify both records exist
	names := []string{records[0]["name"].(string), records[1]["name"].(string)}
	assert.Contains(t, names, "Active Item")
	assert.Contains(t, names, "Deleted Item")

	// Verify history fields removed from both
	for _, record := range records {
		assert.NotContains(t, record, "_fivetran_start")
		assert.NotContains(t, record, "_fivetran_end")
		assert.NotContains(t, record, "_fivetran_active")
	}
}

func TestModeHistoryToLive_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty history-mode table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_table SCHEMAFULL;
		DEFINE FIELD id ON empty_table TYPE array<any>;
		DEFINE FIELD name ON empty_table TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON empty_table TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON empty_table TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON empty_table TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON empty_table TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Convert empty table to live mode
	err = migrator.ModeHistoryToLive(ctx, namespace, "empty_table", false)
	require.NoError(t, err, "ModeHistoryToLive on empty table should not fail")

	// Verify table is still empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM empty_table", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Empty(t, records, "Table should still be empty")

	// Verify schema no longer has history fields
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
	assert.False(t, hasStart, "_fivetran_start should not exist in schema")
	assert.False(t, hasEnd, "_fivetran_end should not exist in schema")
	assert.False(t, hasActive, "_fivetran_active should not exist in schema")
}

func TestModeHistoryToLive_PreservesOtherData(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with multiple columns
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE orders SCHEMAFULL;
		DEFINE FIELD id ON orders TYPE array<any>;
		DEFINE FIELD customer ON orders TYPE option<string>;
		DEFINE FIELD total ON orders TYPE option<float>;
		DEFINE FIELD status ON orders TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON orders TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON orders TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON orders TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON orders TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	startTime := models.CustomDateTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
	endTimeMax := models.CustomDateTime{Time: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)}

	_, err = surrealdb.Query[any](ctx, db, `
		CREATE orders:['order1', d'2024-01-01T00:00:00Z'] SET customer = 'John', total = 100.50, status = 'completed', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = $end_max, _fivetran_active = true;
	`, map[string]any{
		"synced":  startTime,
		"start":   startTime,
		"end_max": endTimeMax,
	})
	require.NoError(t, err, "Failed to insert data")

	// Convert to live mode
	err = migrator.ModeHistoryToLive(ctx, namespace, "orders", false)
	require.NoError(t, err, "ModeHistoryToLive failed")

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
}
