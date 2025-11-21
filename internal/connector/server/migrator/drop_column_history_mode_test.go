package migrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestDropColumnInHistoryMode_BasicDrop(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based ID
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD id ON users TYPE array<any>;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD age ON users TYPE option<int>;
		DEFINE FIELD _fivetran_synced ON users TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON users TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON users TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON users TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert active records with array-based IDs (pk, _fivetran_start)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE type::thing("users", [1, $start]) SET name = 'Alice', age = 30, _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
		CREATE type::thing("users", [2, $start]) SET name = 'Bob', age = 25, _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
	`, map[string]any{
		"synced": baseTime,
		"start":  baseTime,
	})
	require.NoError(t, err, "Failed to insert data")

	// Drop the age column
	operationTimestamp := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	err = migrator.DropColumnInHistoryMode(ctx, namespace, "users", "age", operationTimestamp)
	require.NoError(t, err, "DropColumnInHistoryMode failed")

	// Verify we now have 4 records (2 original deactivated + 2 new active)
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id, _fivetran_active", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 4, "Expected 4 records (2 old + 2 new)")

	// Verify deactivated records and active records
	deactivated := 0
	active := 0
	for _, record := range records {
		if record["_fivetran_active"].(bool) {
			active++
			// Active records should NOT have the age column (or it should be NONE)
			_, hasAge := record["age"]
			assert.False(t, hasAge, "Active record should not have age column")
			startTime := record["_fivetran_start"].(models.CustomDateTime).Time
			assert.Equal(t, operationTimestamp, startTime, "Active record should have _fivetran_start = operation_timestamp")
		} else {
			deactivated++
			// Deactivated records should have _fivetran_end = operation_timestamp - 1ms
			endTime := record["_fivetran_end"].(models.CustomDateTime).Time
			expectedEnd := operationTimestamp.Add(-time.Millisecond)
			assert.Equal(t, expectedEnd, endTime, "Deactivated record should have _fivetran_end = operation_timestamp - 1ms")
			// Deactivated records should still have the age column
			assert.Contains(t, record, "age", "Deactivated record should still have age column")
		}
	}
	assert.Equal(t, 2, deactivated, "Should have 2 deactivated records")
	assert.Equal(t, 2, active, "Should have 2 active records")
}

func TestDropColumnInHistoryMode_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty history-mode table with array-based ID
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_users SCHEMAFULL;
		DEFINE FIELD id ON empty_users TYPE array<any>;
		DEFINE FIELD name ON empty_users TYPE option<string>;
		DEFINE FIELD age ON empty_users TYPE option<int>;
		DEFINE FIELD _fivetran_synced ON empty_users TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON empty_users TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON empty_users TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON empty_users TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Drop column on empty table should succeed (nothing to do)
	operationTimestamp := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	err = migrator.DropColumnInHistoryMode(ctx, namespace, "empty_users", "age", operationTimestamp)
	require.NoError(t, err, "DropColumnInHistoryMode on empty table should not fail")
}

func TestDropColumnInHistoryMode_NoActiveRecords(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based ID
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE archived SCHEMAFULL;
		DEFINE FIELD id ON archived TYPE array<any>;
		DEFINE FIELD name ON archived TYPE option<string>;
		DEFINE FIELD status ON archived TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON archived TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON archived TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON archived TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON archived TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert only inactive records
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE type::thing("archived", [1, $start]) SET name = 'Old Record', status = 'archived', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = $end, _fivetran_active = false;
	`, map[string]any{
		"synced": baseTime,
		"start":  baseTime,
		"end":    baseTime.Add(time.Hour),
	})
	require.NoError(t, err, "Failed to insert data")

	// Drop column on table with no active records
	operationTimestamp := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	err = migrator.DropColumnInHistoryMode(ctx, namespace, "archived", "status", operationTimestamp)
	require.NoError(t, err, "DropColumnInHistoryMode on table with no active records should not fail")

	// Verify record count unchanged
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM archived", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 1, "Should still have 1 record")
}

func TestDropColumnInHistoryMode_InvalidTimestamp(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based ID
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD id ON products TYPE array<any>;
		DEFINE FIELD name ON products TYPE option<string>;
		DEFINE FIELD price ON products TYPE option<float>;
		DEFINE FIELD _fivetran_synced ON products TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON products TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON products TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON products TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert active record with _fivetran_start in the future relative to operation_timestamp
	futureTime := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE type::thing("products", [1, $start]) SET name = 'Product A', price = 10.0, _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
	`, map[string]any{
		"synced": futureTime,
		"start":  futureTime,
	})
	require.NoError(t, err, "Failed to insert data")

	// Try to drop column with operation_timestamp before max(_fivetran_start)
	operationTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	err = migrator.DropColumnInHistoryMode(ctx, namespace, "products", "price", operationTimestamp)
	require.Error(t, err, "Should fail when operation_timestamp < max(_fivetran_start)")
	assert.Contains(t, err.Error(), "must be after max(_fivetran_start)")
}

func TestDropColumnInHistoryMode_ColumnAlreadyNone(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based ID
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD id ON items TYPE array<any>;
		DEFINE FIELD name ON items TYPE option<string>;
		DEFINE FIELD optional_field ON items TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON items TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON items TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON items TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON items TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert active records - some with optional_field, some without
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE type::thing("items", [1, $start]) SET name = 'Item 1', optional_field = 'has value', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
		CREATE type::thing("items", [2, $start]) SET name = 'Item 2', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
	`, map[string]any{
		"synced": baseTime,
		"start":  baseTime,
	})
	require.NoError(t, err, "Failed to insert data")

	// Drop the optional_field column
	operationTimestamp := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	err = migrator.DropColumnInHistoryMode(ctx, namespace, "items", "optional_field", operationTimestamp)
	require.NoError(t, err, "DropColumnInHistoryMode failed")

	// Verify: should have 3 records (1 deactivated, 1 new active from Item 1, 1 unchanged active from Item 2)
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY id, _fivetran_start", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records")

	// Count active records
	activeCount := 0
	for _, record := range records {
		if record["_fivetran_active"].(bool) {
			activeCount++
		}
	}
	assert.Equal(t, 2, activeCount, "Should have 2 active records")
}

func TestDropColumnInHistoryMode_WithInactiveRecords(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based ID
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE orders SCHEMAFULL;
		DEFINE FIELD id ON orders TYPE array<any>;
		DEFINE FIELD name ON orders TYPE option<string>;
		DEFINE FIELD status ON orders TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON orders TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON orders TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON orders TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON orders TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert both active and inactive records with array-based IDs
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	oldStart := baseTime.Add(-24 * time.Hour)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE type::thing("orders", [1, $start]) SET name = 'Order 1', status = 'active', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
		CREATE type::thing("orders", [2, $old_start]) SET name = 'Order 2 (old)', status = 'archived', _fivetran_synced = $synced, _fivetran_start = $old_start, _fivetran_end = $start, _fivetran_active = false;
	`, map[string]any{
		"synced":    baseTime,
		"start":     baseTime,
		"old_start": oldStart,
	})
	require.NoError(t, err, "Failed to insert data")

	// Drop column
	operationTimestamp := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	err = migrator.DropColumnInHistoryMode(ctx, namespace, "orders", "status", operationTimestamp)
	require.NoError(t, err, "DropColumnInHistoryMode failed")

	// Verify: should have 3 records now (1 original inactive, 1 original deactivated, 1 new active)
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders ORDER BY id, _fivetran_start", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records")

	// Count by status
	activeWithoutStatus := 0
	for _, record := range records {
		_, hasStatus := record["status"]
		if record["_fivetran_active"].(bool) && !hasStatus {
			activeWithoutStatus++
		}
	}
	assert.Equal(t, 1, activeWithoutStatus, "Should have 1 active record without status")
}
