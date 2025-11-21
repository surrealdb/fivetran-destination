package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestBatchUpdateIDs_BasicUpdate(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE items SCHEMAFULL;
		DEFINE FIELD name ON items TYPE option<string>;
		DEFINE FIELD value ON items TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert records
	for i := 1; i <= 5; i++ {
		_, err = surrealdb.Query[any](ctx, db, `CREATE type::thing("items", $id) SET name = $name, value = $value`, map[string]any{
			"id":    i,
			"name":  "Item",
			"value": i * 10,
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Update IDs by appending "_v2" (cast to string for numeric IDs)
	err = migrator.BatchUpdateIDs(ctx, "items", "id, name, value", `<string>record::id(id) + "_v2"`, "name, value", 2)
	require.NoError(t, err, "BatchUpdateIDs failed")

	// Verify records have new IDs
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM items ORDER BY value", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 5, "Should have 5 records")

	// Check that IDs contain "_v2"
	for _, record := range records {
		id := record["id"].(models.RecordID)
		assert.Contains(t, id.ID, "_v2", "ID should contain _v2 suffix")
	}
}

func TestBatchUpdateIDs_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_items SCHEMAFULL;
		DEFINE FIELD name ON empty_items TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Update IDs on empty table (cast to string for numeric IDs)
	err = migrator.BatchUpdateIDs(ctx, "empty_items", "id, name", `<string>record::id(id) + "_new"`, "name", 10)
	require.NoError(t, err, "BatchUpdateIDs on empty table should not fail")
}

func TestBatchUpdateIDs_PreservesData(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with multiple fields
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE products SCHEMAFULL;
		DEFINE FIELD name ON products TYPE option<string>;
		DEFINE FIELD price ON products TYPE option<float>;
		DEFINE FIELD stock ON products TYPE option<int>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert records
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE products:1 SET name = 'Product A', price = 10.5, stock = 100;
		CREATE products:2 SET name = 'Product B', price = 20.0, stock = 50;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Update IDs (cast to string for numeric IDs)
	err = migrator.BatchUpdateIDs(ctx, "products", "id, name, price, stock", `<string>record::id(id) + "_updated"`, "name, price, stock", 10)
	require.NoError(t, err, "BatchUpdateIDs failed")

	// Verify data is preserved
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM products ORDER BY name", nil)
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 2, "Should have 2 records")

	// Check first record
	assert.Equal(t, "Product A", records[0]["name"])
	assert.Equal(t, float32(10.5), records[0]["price"])
	assert.Equal(t, uint64(100), records[0]["stock"])

	// Check second record
	assert.Equal(t, "Product B", records[1]["name"])
	assert.Equal(t, float32(20.0), records[1]["price"])
	assert.Equal(t, uint64(50), records[1]["stock"])
}
