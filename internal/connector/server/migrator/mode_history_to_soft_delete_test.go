package migrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestModeHistoryToSoftDelete_BasicConversion(t *testing.T) {
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

	// Insert history-mode data - only latest versions (one per pk)
	startTime1 := models.CustomDateTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
	startTime2 := models.CustomDateTime{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}
	endTimeMax := models.CustomDateTime{Time: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)}

	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:['alice', d'2024-01-01T00:00:00Z'] SET name = 'Alice', email = 'alice@example.com', _fivetran_synced = $synced1, _fivetran_start = $start1, _fivetran_end = $end_max, _fivetran_active = true;
		CREATE users:['bob', d'2024-01-02T00:00:00Z'] SET name = 'Bob', email = 'bob@example.com', _fivetran_synced = $synced2, _fivetran_start = $start2, _fivetran_end = $end_max, _fivetran_active = false;
	`, map[string]any{
		"synced1": startTime1,
		"synced2": startTime2,
		"start1":  startTime1,
		"start2":  startTime2,
		"end_max": endTimeMax,
	})
	require.NoError(t, err, "Failed to insert data")

	// Convert to soft delete mode
	err = migrator.ModeHistoryToSoftDelete(ctx, namespace, "users", "_fivetran_deleted")
	require.NoError(t, err, "ModeHistoryToSoftDelete failed")

	// Verify records exist with simplified IDs
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

	// Verify soft delete column is added and set correctly
	// _fivetran_active = true -> _fivetran_deleted = false
	// _fivetran_active = false -> _fivetran_deleted = true
	assert.Contains(t, records[0], "_fivetran_deleted", "Should have _fivetran_deleted")
	assert.False(t, records[0]["_fivetran_deleted"].(bool), "Alice should not be deleted (was active)")
	assert.True(t, records[1]["_fivetran_deleted"].(bool), "Bob should be deleted (was inactive)")

	// Verify history fields are removed
	assert.NotContains(t, records[0], "_fivetran_start", "Should not have _fivetran_start")
	assert.NotContains(t, records[0], "_fivetran_end", "Should not have _fivetran_end")
	assert.NotContains(t, records[0], "_fivetran_active", "Should not have _fivetran_active")
	assert.Contains(t, records[0], "_fivetran_synced", "Should still have _fivetran_synced")

	// Verify IDs are now simple arrays without _fivetran_start
	id0 := records[0]["id"].(models.RecordID)
	id0Arr := id0.ID.([]any)
	assert.Len(t, id0Arr, 1, "ID should have 1 element (pk only)")
	assert.Equal(t, "alice", id0Arr[0])

	// Verify schema has soft delete field and no history fields
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE users", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasDeleted := fields["_fivetran_deleted"]
	_, hasStart := fields["_fivetran_start"]
	_, hasEnd := fields["_fivetran_end"]
	_, hasActive := fields["_fivetran_active"]
	assert.True(t, hasDeleted, "_fivetran_deleted should exist in schema")
	assert.False(t, hasStart, "_fivetran_start should not exist in schema")
	assert.False(t, hasEnd, "_fivetran_end should not exist in schema")
	assert.False(t, hasActive, "_fivetran_active should not exist in schema")
}

func TestModeHistoryToSoftDelete_DeletesHistoricalVersions(t *testing.T) {
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

	// Insert multiple versions for the same pk (historical versions)
	startTime1 := models.CustomDateTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
	startTime2 := models.CustomDateTime{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}
	startTime3 := models.CustomDateTime{Time: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)}
	endTimeMax := models.CustomDateTime{Time: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)}

	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:['prod1', d'2024-01-01T00:00:00Z'] SET name = 'Product v1', _fivetran_synced = $synced1, _fivetran_start = $start1, _fivetran_end = $start2, _fivetran_active = false;
		CREATE products:['prod1', d'2024-01-02T00:00:00Z'] SET name = 'Product v2', _fivetran_synced = $synced2, _fivetran_start = $start2, _fivetran_end = $start3, _fivetran_active = false;
		CREATE products:['prod1', d'2024-01-03T00:00:00Z'] SET name = 'Product v3', _fivetran_synced = $synced3, _fivetran_start = $start3, _fivetran_end = $end_max, _fivetran_active = true;
	`, map[string]any{
		"synced1": startTime1,
		"synced2": startTime2,
		"synced3": startTime3,
		"start1":  startTime1,
		"start2":  startTime2,
		"start3":  startTime3,
		"end_max": endTimeMax,
	})
	require.NoError(t, err, "Failed to insert data")

	// Verify we have 3 records before conversion
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products", nil)
	require.NoError(t, err)
	require.Len(t, (*results)[0].Result, 3, "Should have 3 historical versions")

	// Convert to soft delete mode
	err = migrator.ModeHistoryToSoftDelete(ctx, namespace, "products", "_fivetran_deleted")
	require.NoError(t, err, "ModeHistoryToSoftDelete failed")

	// Verify only the latest version remains
	results, err = surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 1, "Expected only 1 record (latest version)")

	// Verify it's the latest version
	assert.Equal(t, "Product v3", records[0]["name"])
	assert.False(t, records[0]["_fivetran_deleted"].(bool), "Latest version was active, so not deleted")
}

func TestModeHistoryToSoftDelete_EmptyTable(t *testing.T) {
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

	// Convert empty table
	err = migrator.ModeHistoryToSoftDelete(ctx, namespace, "empty_table", "_fivetran_deleted")
	require.NoError(t, err, "ModeHistoryToSoftDelete on empty table should not fail")

	// Verify table is still empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM empty_table", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Empty(t, records, "Table should still be empty")

	// Verify schema has soft delete field
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_table", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasDeleted := fields["_fivetran_deleted"]
	assert.True(t, hasDeleted, "_fivetran_deleted should exist in schema")
}

func TestModeHistoryToSoftDelete_PreservesOtherData(t *testing.T) {
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

	// Convert to soft delete mode
	err = migrator.ModeHistoryToSoftDelete(ctx, namespace, "orders", "_fivetran_deleted")
	require.NoError(t, err, "ModeHistoryToSoftDelete failed")

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
	assert.Contains(t, record, "_fivetran_deleted")
	assert.False(t, record["_fivetran_deleted"].(bool))
}
