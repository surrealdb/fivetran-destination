package migrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

func TestAddColumnInHistoryMode_BasicAdd(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based ID
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD id ON users TYPE array<any>;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON users TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON users TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON users TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON users TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert active records with array-based IDs (pk, _fivetran_start)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE type::thing("users", [1, $start]) SET name = 'Alice', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
		CREATE type::thing("users", [2, $start]) SET name = 'Bob', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
	`, map[string]any{
		"synced": baseTime,
		"start":  baseTime,
	})
	require.NoError(t, err, "Failed to insert data")

	// Add new column with default value
	operationTimestamp := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	column := &pb.Column{
		Name: "age",
		Type: pb.DataType_INT,
	}
	err = migrator.AddColumnInHistoryMode(ctx, namespace, "users", column, "25", operationTimestamp)
	require.NoError(t, err, "AddColumnInHistoryMode failed")

	// Verify we now have 4 records (2 original deactivated + 2 new active)
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id, _fivetran_active", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 4, "Expected 4 records (2 old + 2 new)")

	// Verify deactivated records
	deactivated := 0
	active := 0
	for _, record := range records {
		if record["_fivetran_active"].(bool) {
			active++
			// Active records should have the new column
			assert.Equal(t, uint64(25), record["age"], "Active record should have age = 25")
			startTime := record["_fivetran_start"].(models.CustomDateTime).Time
			assert.Equal(t, operationTimestamp, startTime, "Active record should have _fivetran_start = operation_timestamp")
		} else {
			deactivated++
			// Deactivated records should have _fivetran_end = operation_timestamp - 1ms
			endTime := record["_fivetran_end"].(models.CustomDateTime).Time
			expectedEnd := operationTimestamp.Add(-time.Millisecond)
			assert.Equal(t, expectedEnd, endTime, "Deactivated record should have _fivetran_end = operation_timestamp - 1ms")
		}
	}
	assert.Equal(t, 2, deactivated, "Should have 2 deactivated records")
	assert.Equal(t, 2, active, "Should have 2 active records")
}

func TestAddColumnInHistoryMode_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty history-mode table with array-based ID
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_users SCHEMAFULL;
		DEFINE FIELD id ON empty_users TYPE array<any>;
		DEFINE FIELD name ON empty_users TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON empty_users TYPE option<datetime>;
		DEFINE FIELD _fivetran_start ON empty_users TYPE option<datetime>;
		DEFINE FIELD _fivetran_end ON empty_users TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON empty_users TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Add column to empty table
	operationTimestamp := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	column := &pb.Column{
		Name: "age",
		Type: pb.DataType_INT,
	}
	err = migrator.AddColumnInHistoryMode(ctx, namespace, "empty_users", column, "30", operationTimestamp)
	require.NoError(t, err, "AddColumnInHistoryMode on empty table should not fail")

	// Verify field was added to schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_users", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasAgeField := fields["age"]
	assert.True(t, hasAgeField, "age field should exist in schema")
}

func TestAddColumnInHistoryMode_InvalidTimestamp(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based ID
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

	// Insert active record with _fivetran_start in the future relative to operation_timestamp
	futureTime := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE type::thing("products", [1, $start]) SET name = 'Product A', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
	`, map[string]any{
		"synced": futureTime,
		"start":  futureTime,
	})
	require.NoError(t, err, "Failed to insert data")

	// Try to add column with operation_timestamp before max(_fivetran_start)
	operationTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	column := &pb.Column{
		Name: "price",
		Type: pb.DataType_FLOAT,
	}
	err = migrator.AddColumnInHistoryMode(ctx, namespace, "products", column, "10.0", operationTimestamp)
	require.Error(t, err, "Should fail when operation_timestamp < max(_fivetran_start)")
	assert.Contains(t, err.Error(), "must be after max(_fivetran_start)")
}

func TestAddColumnInHistoryMode_StringColumn(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based ID
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

	// Insert active record with array-based ID
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE type::thing("items", [1, $start]) SET name = 'Item 1', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
	`, map[string]any{
		"synced": baseTime,
		"start":  baseTime,
	})
	require.NoError(t, err, "Failed to insert data")

	// Add string column
	operationTimestamp := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	column := &pb.Column{
		Name: "description",
		Type: pb.DataType_STRING,
	}
	err = migrator.AddColumnInHistoryMode(ctx, namespace, "items", column, "default description", operationTimestamp)
	require.NoError(t, err, "AddColumnInHistoryMode failed")

	// Verify active record has the new column
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items WHERE _fivetran_active = true", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 1, "Expected 1 active record")

	assert.Equal(t, "default description", records[0]["description"], "Active record should have the default description")
}

func TestAddColumnInHistoryMode_WithInactiveRecords(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create history-mode table with array-based ID
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE orders SCHEMAFULL;
		DEFINE FIELD id ON orders TYPE array<any>;
		DEFINE FIELD name ON orders TYPE option<string>;
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
		CREATE type::thing("orders", [1, $start]) SET name = 'Order 1', _fivetran_synced = $synced, _fivetran_start = $start, _fivetran_end = d'9999-12-31T23:59:59Z', _fivetran_active = true;
		CREATE type::thing("orders", [2, $old_start]) SET name = 'Order 2 (old)', _fivetran_synced = $synced, _fivetran_start = $old_start, _fivetran_end = $start, _fivetran_active = false;
	`, map[string]any{
		"synced":    baseTime,
		"start":     baseTime,
		"old_start": oldStart,
	})
	require.NoError(t, err, "Failed to insert data")

	// Add column
	operationTimestamp := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	column := &pb.Column{
		Name: "status",
		Type: pb.DataType_STRING,
	}
	err = migrator.AddColumnInHistoryMode(ctx, namespace, "orders", column, "pending", operationTimestamp)
	require.NoError(t, err, "AddColumnInHistoryMode failed")

	// Verify: should have 3 records now (1 original inactive, 1 original deactivated, 1 new active)
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders ORDER BY id, _fivetran_start", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records")

	// Count by status
	activeWithStatus := 0
	for _, record := range records {
		if record["_fivetran_active"].(bool) && record["status"] != nil {
			activeWithStatus++
			assert.Equal(t, "pending", record["status"], "Active record should have status = pending")
		}
	}
	assert.Equal(t, 1, activeWithStatus, "Should have 1 active record with status")
}
