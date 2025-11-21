package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestDropTable_BasicDrop(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create table with data
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE users SCHEMAFULL;
		DEFINE FIELD name ON users TYPE option<string>;
		DEFINE FIELD email ON users TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE users:1 SET name = 'Alice', email = 'alice@example.com';
		CREATE users:2 SET name = 'Bob', email = 'bob@example.com';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Drop the table
	err = migrator.DropTable(ctx, namespace, "users")
	require.NoError(t, err, "DropTable failed")

	// Verify table no longer exists by checking INFO FOR DB
	type InfoForDBResult struct {
		Tables map[string]string `cbor:"tables"`
	}
	infoResults, err := surrealdb.Query[InfoForDBResult](ctx, db, "INFO FOR DB", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	tables := (*infoResults)[0].Result.Tables
	_, hasUsers := tables["users"]
	assert.False(t, hasUsers, "users table should not exist")
}

func TestDropTable_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE empty_table SCHEMAFULL;
		DEFINE FIELD name ON empty_table TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create table")

	// Drop the empty table
	err = migrator.DropTable(ctx, namespace, "empty_table")
	require.NoError(t, err, "DropTable on empty table should not fail")

	// Verify table no longer exists
	type InfoForDBResult struct {
		Tables map[string]string `cbor:"tables"`
	}
	infoResults, err := surrealdb.Query[InfoForDBResult](ctx, db, "INFO FOR DB", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	tables := (*infoResults)[0].Result.Tables
	_, hasTable := tables["empty_table"]
	assert.False(t, hasTable, "empty_table should not exist")
}

func TestDropTable_OtherTablesUnaffected(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create multiple tables
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE table_a SCHEMAFULL;
		DEFINE FIELD name ON table_a TYPE option<string>;
		DEFINE TABLE table_b SCHEMAFULL;
		DEFINE FIELD name ON table_b TYPE option<string>;
		DEFINE TABLE table_c SCHEMAFULL;
		DEFINE FIELD name ON table_c TYPE option<string>;
	`, nil)
	require.NoError(t, err, "Failed to create tables")

	// Insert data into all tables
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE table_a:1 SET name = 'A';
		CREATE table_b:1 SET name = 'B';
		CREATE table_c:1 SET name = 'C';
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Drop only table_b
	err = migrator.DropTable(ctx, namespace, "table_b")
	require.NoError(t, err, "DropTable failed")

	// Verify table_b is gone but others remain
	type InfoForDBResult struct {
		Tables map[string]string `cbor:"tables"`
	}
	infoResults, err := surrealdb.Query[InfoForDBResult](ctx, db, "INFO FOR DB", nil)
	require.NoError(t, err)
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	tables := (*infoResults)[0].Result.Tables
	_, hasTableA := tables["table_a"]
	_, hasTableB := tables["table_b"]
	_, hasTableC := tables["table_c"]
	assert.True(t, hasTableA, "table_a should still exist")
	assert.False(t, hasTableB, "table_b should not exist")
	assert.True(t, hasTableC, "table_c should still exist")

	// Verify data in remaining tables is intact
	resultsA, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM table_a", nil)
	require.NoError(t, err)
	require.NotNil(t, resultsA)
	require.NotEmpty(t, *resultsA)
	assert.Len(t, (*resultsA)[0].Result, 1, "table_a should have 1 record")

	resultsC, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM table_c", nil)
	require.NoError(t, err)
	require.NotNil(t, resultsC)
	require.NotEmpty(t, *resultsC)
	assert.Len(t, (*resultsC)[0].Result, 1, "table_c should have 1 record")
}
