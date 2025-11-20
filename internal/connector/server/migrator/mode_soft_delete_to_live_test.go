package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestModeSoftDeleteToLive_BasicConversion(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with soft delete column and _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD email ON users TYPE option<string>;
		DEFINE FIELD _fivetran_deleted ON users TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON users TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with some soft-deleted records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET name = 'Alice', email = 'alice@example.com', _fivetran_deleted = false, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE users:2 SET name = 'Bob', email = 'bob@example.com', _fivetran_deleted = true, _fivetran_synced = d'2024-01-02T00:00:00Z';
		CREATE users:3 SET name = 'Charlie', email = 'charlie@example.com', _fivetran_deleted = false, _fivetran_synced = d'2024-01-03T00:00:00Z';
		CREATE users:4 SET name = 'Diana', email = 'diana@example.com', _fivetran_deleted = true, _fivetran_synced = d'2024-01-04T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to live mode
	err = migrator.ModeSoftDeleteToLive(ctx, namespace, "users", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToLive failed")

	// Verify soft-deleted records are removed
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected only 2 records (soft-deleted ones removed)")

	// Verify remaining records
	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, "alice@example.com", records[0]["email"])
	assert.Contains(t, records[0], "_fivetran_synced", "_fivetran_synced should remain")
	assert.Equal(t, "Charlie", records[1]["name"])
	assert.Equal(t, "charlie@example.com", records[1]["email"])
	assert.Contains(t, records[1], "_fivetran_synced", "_fivetran_synced should remain")

	// Verify soft delete column no longer exists in records
	assert.NotContains(t, records[0], "_fivetran_deleted")
	assert.NotContains(t, records[1], "_fivetran_deleted")

	// Verify field was removed from schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE users", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	_, hasDeletedField := fields["_fivetran_deleted"]
	assert.False(t, hasDeletedField, "Soft delete field should not exist in schema")
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should still exist in schema")
}

func TestModeSoftDeleteToLive_AllRecordsActive(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with soft delete column and _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD name ON products TYPE option<string>;
		DEFINE FIELD price ON products TYPE option<float>;
		DEFINE FIELD _fivetran_deleted ON products TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON products TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with all records active (not deleted)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Widget', price = 9.99, _fivetran_deleted = false, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE products:2 SET name = 'Gadget', price = 19.99, _fivetran_deleted = false, _fivetran_synced = d'2024-01-02T00:00:00Z';
		CREATE products:3 SET name = 'Doodad', price = 29.99, _fivetran_deleted = false, _fivetran_synced = d'2024-01-03T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to live mode
	err = migrator.ModeSoftDeleteToLive(ctx, namespace, "products", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToLive with all active records failed")

	// Verify all records remain
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 3, "All 3 records should remain")

	// Verify data integrity and _fivetran_synced remains
	assert.Equal(t, "Widget", records[0]["name"])
	assert.Contains(t, records[0], "_fivetran_synced")
	assert.Equal(t, "Gadget", records[1]["name"])
	assert.Contains(t, records[1], "_fivetran_synced")
	assert.Equal(t, "Doodad", records[2]["name"])
	assert.Contains(t, records[2], "_fivetran_synced")

	// Verify soft delete column doesn't exist
	assert.NotContains(t, records[0], "_fivetran_deleted")
}

func TestModeSoftDeleteToLive_AllRecordsDeleted(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with soft delete column and _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD name ON items TYPE option<string>;
		DEFINE FIELD _fivetran_deleted ON items TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON items TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with all records soft-deleted
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE items:1 SET name = 'Item1', _fivetran_deleted = true, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE items:2 SET name = 'Item2', _fivetran_deleted = true, _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to live mode
	err = migrator.ModeSoftDeleteToLive(ctx, namespace, "items", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToLive with all deleted records failed")

	// Verify all records are removed
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Empty(t, records, "Table should be empty")
}

func TestModeSoftDeleteToLive_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table with soft delete column and _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_table SCHEMAFULL;
		DEFINE FIELD value ON empty_table TYPE option<string>;
		DEFINE FIELD _fivetran_deleted ON empty_table TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON empty_table TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Convert to live mode
	err = migrator.ModeSoftDeleteToLive(ctx, namespace, "empty_table", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToLive on empty table failed")

	// Verify table is still empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM empty_table", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Empty(t, records, "Table should still be empty")

	// Verify field was removed from schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_table", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	_, hasDeletedField := fields["_fivetran_deleted"]
	assert.False(t, hasDeletedField, "Soft delete field should not exist in schema")
	// "value" is a reserved keyword, so SurrealDB uses backticks
	_, hasValueField := fields["`value`"]
	assert.True(t, hasValueField, "Value field should still exist in schema")
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should still exist in schema")
}

func TestModeSoftDeleteToLive_WithNullValues(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with option types allowing null
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE tasks SCHEMAFULL;
		DEFINE FIELD title ON tasks TYPE option<string>;
		DEFINE FIELD _fivetran_deleted ON tasks TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON tasks TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with mixed soft delete states including null
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE tasks:1 SET title = 'Task1', _fivetran_deleted = false, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE tasks:2 SET title = 'Task2', _fivetran_deleted = true, _fivetran_synced = d'2024-01-02T00:00:00Z';
		CREATE tasks:3 SET title = 'Task3', _fivetran_deleted = NONE, _fivetran_synced = d'2024-01-03T00:00:00Z';
		CREATE tasks:4 SET title = 'Task4', _fivetran_deleted = false, _fivetran_synced = d'2024-01-04T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to live mode
	err = migrator.ModeSoftDeleteToLive(ctx, namespace, "tasks", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToLive with null values failed")

	// Verify only the soft-deleted record is removed (Task2)
	// Records with false or null should remain
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM tasks ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records (only true deleted)")

	assert.Equal(t, "Task1", records[0]["title"])
	assert.Contains(t, records[0], "_fivetran_synced")
	assert.Equal(t, "Task3", records[1]["title"])
	assert.Contains(t, records[1], "_fivetran_synced")
	assert.Equal(t, "Task4", records[2]["title"])
	assert.Contains(t, records[2], "_fivetran_synced")
}

func TestModeSoftDeleteToLive_WithMultipleColumns(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with soft delete column, _fivetran_synced, and other columns (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE orders SCHEMAFULL;
		DEFINE FIELD customer ON orders TYPE option<string>;
		DEFINE FIELD total ON orders TYPE option<float>;
		DEFINE FIELD status ON orders TYPE option<string>;
		DEFINE FIELD _fivetran_deleted ON orders TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON orders TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE orders:1 SET customer = 'John', total = 100.0, status = 'completed', _fivetran_deleted = false, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE orders:2 SET customer = 'Jane', total = 200.0, status = 'cancelled', _fivetran_deleted = true, _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to live mode
	err = migrator.ModeSoftDeleteToLive(ctx, namespace, "orders", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToLive with multiple columns failed")

	// Verify results
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 1, "Expected 1 record")

	// Verify the remaining record still has all other fields
	assert.Equal(t, "John", records[0]["customer"])
	assert.Equal(t, float32(100.0), records[0]["total"])
	assert.Equal(t, "completed", records[0]["status"])
	assert.Contains(t, records[0], "_fivetran_synced", "_fivetran_synced should remain")
	assert.NotContains(t, records[0], "_fivetran_deleted", "Soft delete field should be removed")
}

func TestModeSoftDeleteToLive_LargeDataset(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE records SCHEMAFULL;
		DEFINE FIELD value ON records TYPE option<int>;
		DEFINE FIELD _fivetran_deleted ON records TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON records TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert 100 records, half deleted, half active
	for i := 1; i <= 100; i++ {
		deleted := i%2 == 0 // Even IDs are deleted
		_, err = surrealdb.Query[any](ctx, db, "CREATE records SET value = $value, _fivetran_deleted = $deleted, _fivetran_synced = d'2024-01-01T00:00:00Z'", map[string]any{
			"value":   i,
			"deleted": deleted,
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Convert to live mode
	err = migrator.ModeSoftDeleteToLive(ctx, namespace, "records", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToLive with large dataset failed")

	// Verify count - should have 50 records (odd IDs)
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM records", nil)
	require.NoError(t, err, "Failed to query records")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	assert.Len(t, records, 50, "Expected 50 records (half deleted)")

	// Verify no record has the soft delete field but has _fivetran_synced
	for _, record := range records {
		assert.NotContains(t, record, "_fivetran_deleted")
		assert.Contains(t, record, "_fivetran_synced")
	}
}
