package migrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestModeSoftDeleteToHistory_BasicConversion(t *testing.T) {
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
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET name = 'Alice', email = 'alice@example.com', _fivetran_deleted = false, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE users:2 SET name = 'Bob', email = 'bob@example.com', _fivetran_deleted = true, _fivetran_synced = d'2024-01-02T00:00:00Z';
		CREATE users:3 SET name = 'Charlie', email = 'charlie@example.com', _fivetran_deleted = false, _fivetran_synced = d'2024-01-03T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to history mode
	err = migrator.ModeSoftDeleteToHistory(ctx, namespace, "users", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToHistory failed")

	// Verify all records still exist (none deleted)
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected all 3 records to remain")

	// Verify active records have correct history fields
	// Alice (active): _fivetran_start = max_synced, _fivetran_end = 9999-12-31, _fivetran_active = true
	assert.Equal(t, "Alice", records[0]["name"])
	assert.True(t, records[0]["_fivetran_active"].(bool), "Active record should have _fivetran_active = true")
	aliceStart := records[0]["_fivetran_start"].(models.CustomDateTime).Time
	aliceEnd := records[0]["_fivetran_end"].(models.CustomDateTime).Time
	assert.True(t, aliceStart.After(baseTime) || aliceStart.Equal(baseTime), "Active record should have _fivetran_start = max_synced")
	assert.True(t, aliceEnd.Year() == 9999, "Active record should have _fivetran_end in year 9999")
	assert.NotContains(t, records[0], "_fivetran_deleted", "Soft delete column should be removed")
	assert.Contains(t, records[0], "_fivetran_synced", "_fivetran_synced should remain")

	// Verify deleted record has correct history fields
	// Bob (deleted): _fivetran_start = 0001-01-01, _fivetran_end = 0001-01-01, _fivetran_active = false
	assert.Equal(t, "Bob", records[1]["name"])
	assert.False(t, records[1]["_fivetran_active"].(bool), "Deleted record should have _fivetran_active = false")
	bobStart := records[1]["_fivetran_start"].(models.CustomDateTime).Time
	bobEnd := records[1]["_fivetran_end"].(models.CustomDateTime).Time
	assert.True(t, bobStart.Year() == 1, "Deleted record should have _fivetran_start = 0001-01-01")
	assert.True(t, bobEnd.Year() == 1, "Deleted record should have _fivetran_end = 0001-01-01")
	assert.NotContains(t, records[1], "_fivetran_deleted", "Soft delete column should be removed")
	assert.Contains(t, records[1], "_fivetran_synced", "_fivetran_synced should remain")

	// Verify another active record
	assert.Equal(t, "Charlie", records[2]["name"])
	assert.True(t, records[2]["_fivetran_active"].(bool), "Active record should have _fivetran_active = true")
	assert.NotContains(t, records[2], "_fivetran_deleted", "Soft delete column should be removed")
	assert.Contains(t, records[2], "_fivetran_synced", "_fivetran_synced should remain")

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
	_, hasStartField := fields["_fivetran_start"]
	assert.True(t, hasStartField, "_fivetran_start field should exist in schema")
	_, hasEndField := fields["_fivetran_end"]
	assert.True(t, hasEndField, "_fivetran_end field should exist in schema")
	_, hasActiveField := fields["_fivetran_active"]
	assert.True(t, hasActiveField, "_fivetran_active field should exist in schema")
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should remain in schema")
}

func TestModeSoftDeleteToHistory_AllRecordsActive(t *testing.T) {
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

	// Insert data with all active records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Product A', price = 10.0, _fivetran_deleted = false, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE products:2 SET name = 'Product B', price = 20.0, _fivetran_deleted = false, _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to history mode
	err = migrator.ModeSoftDeleteToHistory(ctx, namespace, "products", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToHistory with all active records failed")

	// Verify all records are active
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for i, record := range records {
		assert.True(t, record["_fivetran_active"].(bool), "Record %d should be active", i)
		startTime := record["_fivetran_start"].(models.CustomDateTime).Time
		endTime := record["_fivetran_end"].(models.CustomDateTime).Time
		assert.True(t, startTime.Year() >= 2024, "Record %d should have _fivetran_start = max_synced (year >= 2024)", i)
		assert.True(t, startTime.Day() == 2, "Record %d should have _fivetran_start day = 2 (max _fivetran_synced is 2024-01-02), got day %d", i, startTime.Day())
		assert.True(t, endTime.Year() == 9999, "Record %d should have _fivetran_end in year 9999", i)
		assert.NotContains(t, record, "_fivetran_deleted", "Record %d should not have _fivetran_deleted", i)
		assert.Contains(t, record, "_fivetran_synced", "Record %d should have _fivetran_synced", i)
	}

	// Verify schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE products", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should remain in schema")
}

func TestModeSoftDeleteToHistory_AllRecordsDeleted(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with soft delete column and _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD description ON items TYPE option<string>;
		DEFINE FIELD _fivetran_deleted ON items TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON items TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with all deleted records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE items:1 SET description = 'Item 1', _fivetran_deleted = true, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE items:2 SET description = 'Item 2', _fivetran_deleted = true, _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to history mode
	err = migrator.ModeSoftDeleteToHistory(ctx, namespace, "items", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToHistory with all deleted records failed")

	// Verify all records are inactive
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for i, record := range records {
		assert.False(t, record["_fivetran_active"].(bool), "Record %d should be inactive", i)
		startTime := record["_fivetran_start"].(models.CustomDateTime).Time
		endTime := record["_fivetran_end"].(models.CustomDateTime).Time
		assert.True(t, startTime.Year() == 1, "Record %d should have _fivetran_start = 0001-01-01", i)
		assert.True(t, endTime.Year() == 1, "Record %d should have _fivetran_end = 0001-01-01", i)
		assert.NotContains(t, record, "_fivetran_deleted", "Record %d should not have _fivetran_deleted", i)
		assert.Contains(t, record, "_fivetran_synced", "Record %d should have _fivetran_synced", i)
	}

	// Verify schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE items", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should remain in schema")
}

func TestModeSoftDeleteToHistory_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table with soft delete column and _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_table SCHEMAFULL;
		DEFINE FIELD data ON empty_table TYPE option<string>;
		DEFINE FIELD _fivetran_deleted ON empty_table TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON empty_table TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Convert to history mode
	err = migrator.ModeSoftDeleteToHistory(ctx, namespace, "empty_table", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToHistory on empty table failed")

	// Verify table is still empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM empty_table", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Empty(t, records, "Table should still be empty")

	// Verify field was removed from schema and history fields added
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
	_, hasStartField := fields["_fivetran_start"]
	assert.True(t, hasStartField, "_fivetran_start field should exist in schema")
	_, hasEndField := fields["_fivetran_end"]
	assert.True(t, hasEndField, "_fivetran_end field should exist in schema")
	_, hasActiveField := fields["_fivetran_active"]
	assert.True(t, hasActiveField, "_fivetran_active field should exist in schema")
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should remain in schema")
}

func TestModeSoftDeleteToHistory_WithNullValues(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with soft delete column and _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE tasks SCHEMAFULL;
		DEFINE FIELD title ON tasks TYPE option<string>;
		DEFINE FIELD description ON tasks TYPE option<string>;
		DEFINE FIELD _fivetran_deleted ON tasks TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON tasks TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with null values (NONE in SurrealDB)
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE tasks:1 SET title = 'Task 1', description = NONE, _fivetran_deleted = false, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE tasks:2 SET title = NONE, description = 'Description 2', _fivetran_deleted = true, _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to history mode
	err = migrator.ModeSoftDeleteToHistory(ctx, namespace, "tasks", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToHistory with null values failed")

	// Verify records
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM tasks ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// First record: active with null description
	assert.Equal(t, "Task 1", records[0]["title"])
	assert.True(t, records[0]["_fivetran_active"].(bool))
	assert.NotContains(t, records[0], "description")
	assert.Contains(t, records[0], "_fivetran_synced", "_fivetran_synced should remain")

	// Second record: deleted with null title
	assert.NotContains(t, records[1], "title")
	assert.Equal(t, "Description 2", records[1]["description"])
	assert.False(t, records[1]["_fivetran_active"].(bool))
	assert.Contains(t, records[1], "_fivetran_synced", "_fivetran_synced should remain")

	// Verify schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE tasks", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should remain in schema")
}

func TestModeSoftDeleteToHistory_WithMultipleColumns(t *testing.T) {
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

	// Convert to history mode
	err = migrator.ModeSoftDeleteToHistory(ctx, namespace, "orders", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToHistory with multiple columns failed")

	// Verify results
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM orders ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify the first record still has all other fields and is active
	assert.Equal(t, "John", records[0]["customer"])
	assert.Equal(t, float32(100.0), records[0]["total"])
	assert.Equal(t, "completed", records[0]["status"])
	assert.True(t, records[0]["_fivetran_active"].(bool))
	assert.Contains(t, records[0], "_fivetran_synced", "_fivetran_synced should remain")
	assert.Contains(t, records[0], "_fivetran_start", "_fivetran_start should be added")
	assert.Contains(t, records[0], "_fivetran_end", "_fivetran_end should be added")
	assert.NotContains(t, records[0], "_fivetran_deleted", "Soft delete field should be removed")

	// Verify the second record still has all other fields and is inactive
	assert.Equal(t, "Jane", records[1]["customer"])
	assert.Equal(t, float32(200.0), records[1]["total"])
	assert.Equal(t, "cancelled", records[1]["status"])
	assert.False(t, records[1]["_fivetran_active"].(bool))
	assert.Contains(t, records[1], "_fivetran_synced", "_fivetran_synced should remain")
	assert.NotContains(t, records[1], "_fivetran_deleted", "Soft delete field should be removed")

	// Verify schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE orders", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should remain in schema")
}

func TestModeSoftDeleteToHistory_LargeDataset(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with soft delete column and _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE records SCHEMAFULL;
		DEFINE FIELD value ON records TYPE option<int>;
		DEFINE FIELD _fivetran_deleted ON records TYPE option<bool>;
		DEFINE FIELD _fivetran_synced ON records TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert 100 records (50 active, 50 deleted)
	for i := 1; i <= 100; i++ {
		deleted := i%2 == 0
		query := `CREATE type::thing("records", $id) SET value = $value, _fivetran_deleted = $deleted, _fivetran_synced = $synced`
		_, err = surrealdb.Query[any](ctx, db, query, map[string]any{
			"id":      i,
			"value":   i * 10,
			"deleted": deleted,
			"synced":  time.Date(2024, 1, 1, 0, 0, i, 0, time.UTC),
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Convert to history mode
	err = migrator.ModeSoftDeleteToHistory(ctx, namespace, "records", "_fivetran_deleted")
	require.NoError(t, err, "ModeSoftDeleteToHistory with large dataset failed")

	// Verify all records still exist
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM records ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 100, "Expected 100 records")

	// Verify history fields are set correctly
	activeCount := 0
	inactiveCount := 0
	for _, record := range records {
		assert.Contains(t, record, "_fivetran_active", "Record should have _fivetran_active")
		assert.Contains(t, record, "_fivetran_start", "Record should have _fivetran_start")
		assert.Contains(t, record, "_fivetran_end", "Record should have _fivetran_end")
		assert.Contains(t, record, "_fivetran_synced", "Record should have _fivetran_synced")
		assert.NotContains(t, record, "_fivetran_deleted", "Record should not have _fivetran_deleted")

		if record["_fivetran_active"].(bool) {
			activeCount++
		} else {
			inactiveCount++
		}
	}

	assert.Equal(t, 50, activeCount, "Expected 50 active records")
	assert.Equal(t, 50, inactiveCount, "Expected 50 inactive records")

	// Verify schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE records", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should remain in schema")
}
