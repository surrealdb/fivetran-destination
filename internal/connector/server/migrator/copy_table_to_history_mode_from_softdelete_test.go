package migrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestCopyTableToHistoryMode_FromSoftDelete_BasicConversion(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create soft-delete mode source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD email ON source TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON source TYPE option<datetime>;
		DEFINE FIELD _fivetran_deleted ON source TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert soft-delete mode data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:['alice'] SET name = 'Alice', email = 'alice@example.com', _fivetran_synced = d'2024-01-01T00:00:00Z', _fivetran_deleted = false;
		CREATE source:['bob'] SET name = 'Bob', email = 'bob@example.com', _fivetran_synced = d'2024-01-02T00:00:00Z', _fivetran_deleted = true;
		CREATE source:['charlie'] SET name = 'Charlie', email = 'charlie@example.com', _fivetran_synced = d'2024-01-03T00:00:00Z', _fivetran_deleted = false;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy to history mode
	err = migrator.CopyTableToHistoryMode(ctx, namespace, "", "source", "dest", "_fivetran_deleted")
	require.NoError(t, err, "CopyTableToHistoryMode failed")

	// Verify destination table has history mode records
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 3, "Expected 3 records in dest")

	// Verify data preserved
	assert.Equal(t, "Alice", records[0]["name"])
	assert.Equal(t, "alice@example.com", records[0]["email"])
	assert.Equal(t, "Bob", records[1]["name"])
	assert.Equal(t, "bob@example.com", records[1]["email"])
	assert.Equal(t, "Charlie", records[2]["name"])
	assert.Equal(t, "charlie@example.com", records[2]["email"])

	// Verify history fields are added
	assert.Contains(t, records[0], "_fivetran_start", "Should have _fivetran_start")
	assert.Contains(t, records[0], "_fivetran_end", "Should have _fivetran_end")
	assert.Contains(t, records[0], "_fivetran_active", "Should have _fivetran_active")
	assert.Contains(t, records[0], "_fivetran_synced", "Should still have _fivetran_synced")

	// Verify _fivetran_deleted is removed
	assert.NotContains(t, records[0], "_fivetran_deleted", "Should not have _fivetran_deleted")

	// Verify _fivetran_active is set correctly (NOT of _fivetran_deleted)
	// Alice: _fivetran_deleted = false -> _fivetran_active = true
	// Bob: _fivetran_deleted = true -> _fivetran_active = false
	// Charlie: _fivetran_deleted = false -> _fivetran_active = true
	assert.True(t, records[0]["_fivetran_active"].(bool), "Alice should be active")
	assert.False(t, records[1]["_fivetran_active"].(bool), "Bob should be inactive (was deleted)")
	assert.True(t, records[2]["_fivetran_active"].(bool), "Charlie should be active")

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
}

func TestCopyTableToHistoryMode_FromSoftDelete_EmptyTable(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create empty soft-delete mode source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD name ON source TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON source TYPE option<datetime>;
		DEFINE FIELD _fivetran_deleted ON source TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Copy empty table to history mode
	err = migrator.CopyTableToHistoryMode(ctx, namespace, "", "source", "dest", "_fivetran_deleted")
	require.NoError(t, err, "CopyTableToHistoryMode on empty table should not fail")

	// Verify dest table is empty
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Empty(t, records, "Dest table should be empty")

	// Verify schema has history fields
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, db, "INFO FOR TABLE dest", nil)
	require.NoError(t, err, "Failed to get table info")
	require.NotNil(t, infoResults)
	require.NotEmpty(t, *infoResults)

	fields := (*infoResults)[0].Result.Fields
	_, hasStart := fields["_fivetran_start"]
	_, hasEnd := fields["_fivetran_end"]
	_, hasActive := fields["_fivetran_active"]
	_, hasDeleted := fields["_fivetran_deleted"]
	assert.True(t, hasStart, "_fivetran_start should exist in schema")
	assert.True(t, hasEnd, "_fivetran_end should exist in schema")
	assert.True(t, hasActive, "_fivetran_active should exist in schema")
	assert.False(t, hasDeleted, "_fivetran_deleted should not exist in schema")
}

func TestCopyTableToHistoryMode_FromSoftDelete_PreservesOtherData(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create soft-delete mode source table with multiple columns
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD customer ON source TYPE option<string>;
		DEFINE FIELD total ON source TYPE option<float>;
		DEFINE FIELD status ON source TYPE option<string>;
		DEFINE FIELD _fivetran_synced ON source TYPE option<datetime>;
		DEFINE FIELD _fivetran_deleted ON source TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert data
	_, err = surrealdb.Query[any](ctx, db, `
		CREATE source:['order1'] SET customer = 'John', total = 100.50, status = 'completed', _fivetran_synced = d'2024-01-01T00:00:00Z', _fivetran_deleted = false;
	`, nil)
	require.NoError(t, err, "Failed to insert data")

	// Copy to history mode
	err = migrator.CopyTableToHistoryMode(ctx, namespace, "", "source", "dest", "_fivetran_deleted")
	require.NoError(t, err, "CopyTableToHistoryMode failed")

	// Verify all non-soft-delete data is preserved
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest", nil)
	require.NoError(t, err, "Failed to query dest table")
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
	assert.NotContains(t, record, "_fivetran_deleted")
	assert.True(t, record["_fivetran_active"].(bool))
}

func TestCopyTableToHistoryMode_FromSoftDelete_SmallBatchSize(t *testing.T) {
	ctx := t.Context()
	namespace := testNamespace(t)

	db, migrator := testSetup(t, namespace)

	// Create soft-delete mode source table
	_, err := surrealdb.Query[any](ctx, db, `
		DEFINE TABLE source SCHEMAFULL;
		DEFINE FIELD id ON source TYPE array<any>;
		DEFINE FIELD value ON source TYPE option<int>;
		DEFINE FIELD _fivetran_deleted ON source TYPE option<bool>;
	`, nil)
	require.NoError(t, err, "Failed to create source table")

	// Insert 10 records
	for i := 1; i <= 10; i++ {
		deleted := i%2 == 0 // Even records are deleted
		_, err = surrealdb.Query[any](ctx, db, "CREATE type::thing('source', [$id]) SET value = $value, _fivetran_deleted = $deleted", map[string]any{
			"id":      i,
			"value":   i * 10,
			"deleted": deleted,
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Copy with small batch size (implicitly uses default from function)
	err = migrator.CopyTableToHistoryMode(ctx, namespace, "", "source", "dest", "_fivetran_deleted")
	require.NoError(t, err, "CopyTableToHistoryMode failed")

	// Verify all 10 records were copied
	results, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM dest ORDER BY id", nil)
	require.NoError(t, err, "Failed to query dest table")
	require.NotNil(t, results)
	require.NotEmpty(t, *results)
	records := (*results)[0].Result
	require.Len(t, records, 10, "Expected 10 records in dest")

	// Verify all values and _fivetran_active states
	for i := range 10 {
		assert.Equal(t, uint64((i+1)*10), records[i]["value"], "Record %d has wrong value", i)
		// Odd records (i=0,2,4,6,8) should be active (were not deleted)
		// Even records (i=1,3,5,7,9) should be inactive (were deleted)
		expectedActive := (i+1)%2 == 1
		assert.Equal(t, expectedActive, records[i]["_fivetran_active"].(bool), "Record %d has wrong _fivetran_active state", i)
	}
}
