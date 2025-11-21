package migrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestModeLiveToSoftDelete_BasicConversion(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table in live mode with _fivetran_synced (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD email ON users TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON users TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET name = 'Alice', email = 'alice@example.com', _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE users:2 SET name = 'Bob', email = 'bob@example.com', _fivetran_synced = d'2024-01-02T00:00:00Z';
		CREATE users:3 SET name = 'Charlie', email = 'charlie@example.com', _fivetran_synced = d'2024-01-03T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to soft delete mode
	err = migrator.ModeLiveToSoftDelete(ctx, namespace, "users", "_fivetran_deleted")
	require.NoError(t, err, "ModeLiveToSoftDelete failed")

	// Verify all records now have soft delete column set to false
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected all 3 records to remain")

	// Verify all records have _fivetran_deleted = false
	for i, record := range records {
		assert.False(t, record["_fivetran_deleted"].(bool), "Record %d should have _fivetran_deleted = false", i)
		assert.Contains(t, record, "_fivetran_synced", "Record %d should have _fivetran_synced", i)
	}

	// Verify soft delete field was added to schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE users", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	_, hasDeletedField := fields["_fivetran_deleted"]
	assert.True(t, hasDeletedField, "Soft delete field should exist in schema")
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should remain in schema")
}

func TestModeLiveToSoftDelete_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table in live mode (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_table SCHEMAFULL;
		DEFINE FIELD data ON empty_table TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON empty_table TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Convert to soft delete mode
	err = migrator.ModeLiveToSoftDelete(ctx, namespace, "empty_table", "_fivetran_deleted")
	require.NoError(t, err, "ModeLiveToSoftDelete on empty table failed")

	// Verify table is still empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM empty_table", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Empty(t, records, "Table should still be empty")

	// Verify soft delete field was added to schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE empty_table", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	fields := (*infoResults)[0].Result.Fields
	_, hasDeletedField := fields["_fivetran_deleted"]
	assert.True(t, hasDeletedField, "Soft delete field should exist in schema")
	_, hasSyncedField := fields["_fivetran_synced"]
	assert.True(t, hasSyncedField, "_fivetran_synced field should remain in schema")
}

func TestModeLiveToSoftDelete_WithMultipleColumns(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with multiple columns (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD name ON products TYPE option<string>;
		DEFINE FIELD price ON products TYPE option<float>;
		DEFINE FIELD stock ON products TYPE option<int>;
		DEFINE FIELD _fivetran_synced ON products TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Product A', price = 10.0, stock = 100, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE products:2 SET name = 'Product B', price = 20.0, stock = 50, _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to soft delete mode
	err = migrator.ModeLiveToSoftDelete(ctx, namespace, "products", "_fivetran_deleted")
	require.NoError(t, err, "ModeLiveToSoftDelete with multiple columns failed")

	// Verify all records have soft delete column
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify first record
	assert.Equal(t, "Product A", records[0]["name"])
	assert.Equal(t, float32(10.0), records[0]["price"])
	assert.Equal(t, uint64(100), records[0]["stock"])
	assert.False(t, records[0]["_fivetran_deleted"].(bool))
	assert.Contains(t, records[0], "_fivetran_synced", "_fivetran_synced should remain")

	// Verify second record
	assert.Equal(t, "Product B", records[1]["name"])
	assert.Equal(t, float32(20.0), records[1]["price"])
	assert.Equal(t, uint64(50), records[1]["stock"])
	assert.False(t, records[1]["_fivetran_deleted"].(bool))
	assert.Contains(t, records[1], "_fivetran_synced", "_fivetran_synced should remain")

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

func TestModeLiveToSoftDelete_WithNoneValues(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with optional fields (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD name ON items TYPE option<string>;
		DEFINE FIELD description ON items TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON items TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data with NONE values
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE items:1 SET name = 'Item 1', description = NONE, _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE items:2 SET name = NONE, description = 'Description 2', _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to soft delete mode
	err = migrator.ModeLiveToSoftDelete(ctx, namespace, "items", "_fivetran_deleted")
	require.NoError(t, err, "ModeLiveToSoftDelete with null values failed")

	// Verify records
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	// Verify null values are preserved
	assert.Equal(t, "Item 1", records[0]["name"])
	assert.NotContains(t, records[0], "description")
	assert.False(t, records[0]["_fivetran_deleted"].(bool))
	assert.Contains(t, records[0], "_fivetran_synced", "_fivetran_synced should remain")

	assert.NotContains(t, records[1], "name")
	assert.Equal(t, "Description 2", records[1]["description"])
	assert.False(t, records[1]["_fivetran_deleted"].(bool))
	assert.Contains(t, records[1], "_fivetran_synced", "_fivetran_synced should remain")

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

func TestModeLiveToSoftDelete_WithIndexes(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with indexes (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD email ON users TYPE option<string>;
		DEFINE FIELD username ON users TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON users TYPE option<datetime>;
		DEFINE INDEX email_idx ON users FIELDS email;
		DEFINE INDEX username_idx ON users FIELDS username;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET email = 'alice@example.com', username = 'alice', _fivetran_synced = d'2024-01-01T00:00:00Z';
		CREATE users:2 SET email = 'bob@example.com', username = 'bob', _fivetran_synced = d'2024-01-02T00:00:00Z';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Convert to soft delete mode
	err = migrator.ModeLiveToSoftDelete(ctx, namespace, "users", "_fivetran_deleted")
	require.NoError(t, err, "ModeLiveToSoftDelete with indexes failed")

	// Verify records
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 2, "Expected 2 records")

	for i, record := range records {
		assert.False(t, record["_fivetran_deleted"].(bool), "Record %d should have _fivetran_deleted = false", i)
		assert.Contains(t, record, "_fivetran_synced", "Record %d should have _fivetran_synced", i)
	}

	// Verify indexes are preserved
	type InfoForTableResult struct {
		Indexes map[string]string `cbor:"indexes"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE users", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults, "Info result is nil")
	require.NotEmpty(t, *infoResults, "Info result is empty")

	indexes := (*infoResults)[0].Result.Indexes
	_, hasEmailIdx := indexes["email_idx"]
	assert.True(t, hasEmailIdx, "email_idx should be preserved")
	_, hasUsernameIdx := indexes["username_idx"]
	assert.True(t, hasUsernameIdx, "username_idx should be preserved")
}

func TestModeLiveToSoftDelete_LargeDataset(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table (all option types)
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE records SCHEMAFULL;
		DEFINE FIELD value ON records TYPE option<int>;
		DEFINE FIELD _fivetran_synced ON records TYPE option<datetime>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert 100 records
	for i := 1; i <= 100; i++ {
		query := `CREATE type::thing("records", $id) SET value = $value, _fivetran_synced = $synced`
		_, err = surrealdb.Query[any](ctx, db, query, map[string]any{
			"id":     i,
			"value":  i * 10,
			"synced": time.Date(2024, 1, 1, 0, 0, i, 0, time.UTC),
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Convert to soft delete mode
	err = migrator.ModeLiveToSoftDelete(ctx, namespace, "records", "_fivetran_deleted")
	require.NoError(t, err, "ModeLiveToSoftDelete with large dataset failed")

	// Verify all records have soft delete column
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM records ORDER BY id", nil)
	require.NoError(t, err, "Failed to query table")
	require.NotNil(t, results, "Query result is nil")
	require.NotEmpty(t, *results, "Query result is empty")
	records := (*results)[0].Result
	require.Len(t, records, 100, "Expected 100 records")

	// Verify all records have _fivetran_deleted = false
	for _, record := range records {
		assert.False(t, record["_fivetran_deleted"].(bool), "Record should have _fivetran_deleted = false")
		assert.Contains(t, record, "_fivetran_synced", "Record should have _fivetran_synced")
	}

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
