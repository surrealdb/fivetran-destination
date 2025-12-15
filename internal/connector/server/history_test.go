package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/surrealdb/fivetran-destination/internal/connector/server/testframework"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func TestHasIdPKColumn(t *testing.T) {
	tests := []struct {
		name      string
		pkColumns []string
		expected  bool
	}{
		{
			name:      "has id column",
			pkColumns: []string{"id", "_fivetran_start"},
			expected:  true,
		},
		{
			name:      "no id column",
			pkColumns: []string{"user_id", "_fivetran_start"},
			expected:  false,
		},
		{
			name:      "id only",
			pkColumns: []string{"id"},
			expected:  true,
		},
		{
			name:      "empty columns",
			pkColumns: []string{},
			expected:  false,
		},
		{
			name:      "id in middle",
			pkColumns: []string{"tenant_id", "id", "_fivetran_start"},
			expected:  true,
		},
		{
			name:      "_fivetran_id is not id",
			pkColumns: []string{"_fivetran_id", "_fivetran_start"},
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasIdPKColumn(tt.pkColumns)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildRangeQueryBounds(t *testing.T) {
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	maxTime := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

	tests := []struct {
		name              string
		pkColumns         []string
		pkValues          []any
		expectedLower     []any
		expectedUpperLen  int
		expectedUpperLast models.CustomDateTime
	}{
		{
			name:              "single pk with fivetran_start",
			pkColumns:         []string{"id", "_fivetran_start"},
			pkValues:          []any{"order1", models.CustomDateTime{Time: ts}},
			expectedLower:     []any{"order1"},
			expectedUpperLen:  2,
			expectedUpperLast: models.CustomDateTime{Time: maxTime},
		},
		{
			name:              "composite pk with fivetran_start",
			pkColumns:         []string{"tenant_id", "id", "_fivetran_start"},
			pkValues:          []any{"tenant1", "order1", models.CustomDateTime{Time: ts}},
			expectedLower:     []any{"tenant1", "order1"},
			expectedUpperLen:  3,
			expectedUpperLast: models.CustomDateTime{Time: maxTime},
		},
		{
			name:              "single pk without fivetran_start",
			pkColumns:         []string{"id"},
			pkValues:          []any{"order1"},
			expectedLower:     []any{"order1"},
			expectedUpperLen:  2,
			expectedUpperLast: models.CustomDateTime{Time: maxTime},
		},
		{
			name:              "_fivetran_id pk with fivetran_start",
			pkColumns:         []string{"_fivetran_id", "_fivetran_start"},
			pkValues:          []any{"user1", models.CustomDateTime{Time: ts}},
			expectedLower:     []any{"user1"},
			expectedUpperLen:  2,
			expectedUpperLast: models.CustomDateTime{Time: maxTime},
		},
		{
			name:              "user_id pk with fivetran_start",
			pkColumns:         []string{"user_id", "_fivetran_start"},
			pkValues:          []any{int64(123), models.CustomDateTime{Time: ts}},
			expectedLower:     []any{int64(123)},
			expectedUpperLen:  2,
			expectedUpperLast: models.CustomDateTime{Time: maxTime},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := buildRecordIDRangeQueryBounds(tt.pkColumns, tt.pkValues)

			assert.Equal(t, tt.expectedLower, config.lowerBound, "lowerBound mismatch")
			assert.Len(t, config.upperBound, tt.expectedUpperLen, "upperBound length mismatch")
			assert.Equal(t, tt.expectedUpperLast, config.upperBound[len(config.upperBound)-1], "upperBound last element should be maxTime")
			assert.Equal(t, "id >= type::thing($tb, $lower) AND id < type::thing($tb, $upper)", config.byID)
		})
	}
}

func TestBuildRangeQueryBounds_ExcludesFivetranStart(t *testing.T) {
	// Verify that _fivetran_start is excluded from the lower bound
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	pkColumns := []string{"id", "_fivetran_start"}
	pkValues := []any{"order1", models.CustomDateTime{Time: ts}}

	config := buildRecordIDRangeQueryBounds(pkColumns, pkValues)

	// Lower bound should only contain "order1", not the timestamp
	require.Len(t, config.lowerBound, 1)
	assert.Equal(t, "order1", config.lowerBound[0])

	// Upper bound should contain "order1" + max datetime
	require.Len(t, config.upperBound, 2)
	assert.Equal(t, "order1", config.upperBound[0])

	maxTime := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	assert.Equal(t, models.CustomDateTime{Time: maxTime}, config.upperBound[1])
}

func TestBuildRangeQueryBounds_PreservesValueTypes(t *testing.T) {
	// Verify that value types are preserved (important for SurrealDB)
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	customDT := models.CustomDateTime{Time: ts}

	pkColumns := []string{"tenant_id", "user_id", "_fivetran_start"}
	pkValues := []any{"tenant1", int64(123), customDT}

	config := buildRecordIDRangeQueryBounds(pkColumns, pkValues)

	require.Len(t, config.lowerBound, 2)
	assert.IsType(t, "", config.lowerBound[0], "first element should be string")
	assert.IsType(t, int64(0), config.lowerBound[1], "second element should be int64")
}

// sanitizeTestName converts a test name into a valid namespace/database name
func sanitizeTestName(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return strings.ToLower(name)
}

// setupHistoryTestDB creates a test database connection for history tests.
// The namespace and database names are derived from the test name.
// Cleanup is automatically registered via t.Cleanup.
func setupHistoryTestDB(t *testing.T) *surrealdb.DB {
	ctx := t.Context()

	name := sanitizeTestName(t.Name())
	db, err := testframework.SetupTestDB(t, name, name)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close(ctx)
	})

	return db
}

// TestRangeQuerySubquery_WithIdColumn tests the range query subquery with "id" as a PK column
func TestRangeQuerySubquery_WithIdColumn(t *testing.T) {
	db := setupHistoryTestDB(t)
	ctx := t.Context()

	tableName := "test_history_id"
	_, err := surrealdb.Query[any](ctx, db, fmt.Sprintf("REMOVE TABLE IF EXISTS %s;", tableName), nil)
	require.NoError(t, err)

	// Define table schema with "id" as a column (simulating source data with id column)
	_, err = surrealdb.Query[any](ctx, db, fmt.Sprintf(`
		DEFINE TABLE %s SCHEMAFULL;
		DEFINE FIELD id ON %s TYPE array;
		DEFINE FIELD _fivetran_start ON %s TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON %s TYPE option<bool>;
		DEFINE FIELD name ON %s TYPE option<string>;
		DEFINE FIELD amount ON %s TYPE option<int>;
	`, tableName, tableName, tableName, tableName, tableName, tableName), nil)
	require.NoError(t, err)

	// Create test records with multiple versions
	ts1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)

	// id1 with two versions
	thing1v1 := models.NewRecordID(tableName, []any{"id1", models.CustomDateTime{Time: ts1}})
	_, err = surrealdb.Upsert[any](ctx, db, thing1v1, map[string]any{
		"_fivetran_start":  models.CustomDateTime{Time: ts1},
		"_fivetran_active": false,
		"name":             "Alice v1",
		"amount":           100,
	})
	require.NoError(t, err)

	thing1v2 := models.NewRecordID(tableName, []any{"id1", models.CustomDateTime{Time: ts2}})
	_, err = surrealdb.Upsert[any](ctx, db, thing1v2, map[string]any{
		"_fivetran_start":  models.CustomDateTime{Time: ts2},
		"_fivetran_active": true,
		"name":             "Alice v2",
		"amount":           150,
	})
	require.NoError(t, err)

	// id2 with one version
	thing2 := models.NewRecordID(tableName, []any{"id2", models.CustomDateTime{Time: ts1}})
	_, err = surrealdb.Upsert[any](ctx, db, thing2, map[string]any{
		"_fivetran_start":  models.CustomDateTime{Time: ts1},
		"_fivetran_active": true,
		"name":             "Bob",
		"amount":           200,
	})
	require.NoError(t, err)

	t.Run("finds_latest_record_for_id1", func(t *testing.T) {
		pkColumns := []string{"id", "_fivetran_start"}
		pkValues := []any{"id1", models.CustomDateTime{Time: ts1}} // Searching for id1

		rangeConfig := buildRecordIDRangeQueryBounds(pkColumns, pkValues)
		query := buildRangeQuerySubquery("_fivetran_start, id, name, amount")

		result, err := surrealdb.Query[[]map[string]any](ctx, db, query, map[string]any{
			"tb":    tableName,
			"lower": rangeConfig.lowerBound,
			"upper": rangeConfig.upperBound,
		})
		require.NoError(t, err)
		require.Len(t, (*result)[0].Result, 1, "Should return exactly 1 record (latest)")

		latest := (*result)[0].Result[0]
		assert.Equal(t, "Alice v2", latest["name"], "Should get the latest version")
		assert.Equal(t, uint64(150), latest["amount"])
	})

	t.Run("finds_record_for_id2", func(t *testing.T) {
		pkColumns := []string{"id", "_fivetran_start"}
		pkValues := []any{"id2", models.CustomDateTime{Time: ts1}}

		rangeConfig := buildRecordIDRangeQueryBounds(pkColumns, pkValues)
		query := buildRangeQuerySubquery("_fivetran_start, id, name, amount")

		result, err := surrealdb.Query[[]map[string]any](ctx, db, query, map[string]any{
			"tb":    tableName,
			"lower": rangeConfig.lowerBound,
			"upper": rangeConfig.upperBound,
		})
		require.NoError(t, err)
		require.Len(t, (*result)[0].Result, 1)

		record := (*result)[0].Result[0]
		assert.Equal(t, "Bob", record["name"])
		assert.Equal(t, uint64(200), record["amount"])
	})

	t.Run("returns_empty_for_nonexistent_id", func(t *testing.T) {
		pkColumns := []string{"id", "_fivetran_start"}
		pkValues := []any{"nonexistent", models.CustomDateTime{Time: ts1}}

		rangeConfig := buildRecordIDRangeQueryBounds(pkColumns, pkValues)
		query := buildRangeQuerySubquery("_fivetran_start, id, name, amount")

		result, err := surrealdb.Query[[]map[string]any](ctx, db, query, map[string]any{
			"tb":    tableName,
			"lower": rangeConfig.lowerBound,
			"upper": rangeConfig.upperBound,
		})
		require.NoError(t, err)
		assert.Len(t, (*result)[0].Result, 0, "Should return no records for nonexistent id")
	})

	t.Run("preserves_full_record_id", func(t *testing.T) {
		pkColumns := []string{"id", "_fivetran_start"}
		pkValues := []any{"id1", models.CustomDateTime{Time: ts1}}

		rangeConfig := buildRecordIDRangeQueryBounds(pkColumns, pkValues)
		query := buildRangeQuerySubquery("id")

		result, err := surrealdb.Query[[]map[string]any](ctx, db, query, map[string]any{
			"tb":    tableName,
			"lower": rangeConfig.lowerBound,
			"upper": rangeConfig.upperBound,
		})
		require.NoError(t, err)
		require.Len(t, (*result)[0].Result, 1)

		// Verify we can extract the full RecordID
		record := (*result)[0].Result[0]
		rid, ok := record["id"].(models.RecordID)
		require.True(t, ok, "id should be a RecordID")
		assert.Equal(t, tableName, rid.Table)

		idArr, ok := rid.ID.([]any)
		require.True(t, ok, "RecordID.ID should be an array")
		assert.Len(t, idArr, 2)
		assert.Equal(t, "id1", idArr[0])
	})
}

// TestRangeQuerySubquery_WithFivetranIdColumn tests with "_fivetran_id" as PK (not "id")
func TestRangeQuerySubquery_WithFivetranIdColumn(t *testing.T) {
	db := setupHistoryTestDB(t)
	ctx := t.Context()

	tableName := "test_history_fivetran_id"
	_, err := surrealdb.Query[any](ctx, db, fmt.Sprintf("REMOVE TABLE IF EXISTS %s;", tableName), nil)
	require.NoError(t, err)

	// Define table schema with "_fivetran_id" as PK (standard Fivetran history mode)
	_, err = surrealdb.Query[any](ctx, db, fmt.Sprintf(`
		DEFINE TABLE %s SCHEMAFULL;
		DEFINE FIELD _fivetran_id ON %s TYPE option<string>;
		DEFINE FIELD _fivetran_start ON %s TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON %s TYPE option<bool>;
		DEFINE FIELD name ON %s TYPE option<string>;
		DEFINE FIELD amount ON %s TYPE option<int>;
	`, tableName, tableName, tableName, tableName, tableName, tableName), nil)
	require.NoError(t, err)

	// Create test records
	ts1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)

	// user1 with two versions
	thing1v1 := models.NewRecordID(tableName, []any{"user1", models.CustomDateTime{Time: ts1}})
	_, err = surrealdb.Upsert[any](ctx, db, thing1v1, map[string]any{
		"_fivetran_id":     "user1",
		"_fivetran_start":  models.CustomDateTime{Time: ts1},
		"_fivetran_active": false,
		"name":             "User1 v1",
		"amount":           100,
	})
	require.NoError(t, err)

	thing1v2 := models.NewRecordID(tableName, []any{"user1", models.CustomDateTime{Time: ts2}})
	_, err = surrealdb.Upsert[any](ctx, db, thing1v2, map[string]any{
		"_fivetran_id":     "user1",
		"_fivetran_start":  models.CustomDateTime{Time: ts2},
		"_fivetran_active": true,
		"name":             "User1 v2",
		"amount":           150,
	})
	require.NoError(t, err)

	t.Run("hasIdPKColumn_returns_false_for_fivetran_id", func(t *testing.T) {
		pkColumns := []string{"_fivetran_id", "_fivetran_start"}
		assert.False(t, hasIdPKColumn(pkColumns), "_fivetran_id should not be detected as 'id' column")
	})

	t.Run("range_query_still_works_for_fivetran_id", func(t *testing.T) {
		// Even though hasIdPKColumn returns false, the range query approach still works
		// This demonstrates the query mechanism is correct regardless of column naming
		pkColumns := []string{"_fivetran_id", "_fivetran_start"}
		pkValues := []any{"user1", models.CustomDateTime{Time: ts1}}

		rangeConfig := buildRecordIDRangeQueryBounds(pkColumns, pkValues)
		query := buildRangeQuerySubquery("_fivetran_start, id, name, amount")

		result, err := surrealdb.Query[[]map[string]any](ctx, db, query, map[string]any{
			"tb":    tableName,
			"lower": rangeConfig.lowerBound,
			"upper": rangeConfig.upperBound,
		})
		require.NoError(t, err)
		require.Len(t, (*result)[0].Result, 1)

		latest := (*result)[0].Result[0]
		assert.Equal(t, "User1 v2", latest["name"], "Should get the latest version")
		assert.Equal(t, uint64(150), latest["amount"])
	})
}

// TestRangeQuerySubquery_WithCompositePK tests with composite PK (tenant_id, id, _fivetran_start)
func TestRangeQuerySubquery_WithCompositePK(t *testing.T) {
	db := setupHistoryTestDB(t)
	ctx := t.Context()

	tableName := "test_history_composite"
	_, err := surrealdb.Query[any](ctx, db, fmt.Sprintf("REMOVE TABLE IF EXISTS %s;", tableName), nil)
	require.NoError(t, err)

	// Define table with composite PK
	_, err = surrealdb.Query[any](ctx, db, fmt.Sprintf(`
		DEFINE TABLE %s SCHEMAFULL;
		DEFINE FIELD tenant_id ON %s TYPE option<string>;
		DEFINE FIELD id ON %s TYPE array;
		DEFINE FIELD _fivetran_start ON %s TYPE option<datetime>;
		DEFINE FIELD _fivetran_active ON %s TYPE option<bool>;
		DEFINE FIELD name ON %s TYPE option<string>;
	`, tableName, tableName, tableName, tableName, tableName, tableName), nil)
	require.NoError(t, err)

	ts1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)

	// tenant1/id1 with two versions
	thing1v1 := models.NewRecordID(tableName, []any{"tenant1", "id1", models.CustomDateTime{Time: ts1}})
	_, err = surrealdb.Upsert[any](ctx, db, thing1v1, map[string]any{
		"tenant_id":        "tenant1",
		"_fivetran_start":  models.CustomDateTime{Time: ts1},
		"_fivetran_active": false,
		"name":             "T1-ID1 v1",
	})
	require.NoError(t, err)

	thing1v2 := models.NewRecordID(tableName, []any{"tenant1", "id1", models.CustomDateTime{Time: ts2}})
	_, err = surrealdb.Upsert[any](ctx, db, thing1v2, map[string]any{
		"tenant_id":        "tenant1",
		"_fivetran_start":  models.CustomDateTime{Time: ts2},
		"_fivetran_active": true,
		"name":             "T1-ID1 v2",
	})
	require.NoError(t, err)

	// tenant1/id2 (different id, same tenant)
	thing2 := models.NewRecordID(tableName, []any{"tenant1", "id2", models.CustomDateTime{Time: ts1}})
	_, err = surrealdb.Upsert[any](ctx, db, thing2, map[string]any{
		"tenant_id":        "tenant1",
		"_fivetran_start":  models.CustomDateTime{Time: ts1},
		"_fivetran_active": true,
		"name":             "T1-ID2",
	})
	require.NoError(t, err)

	// tenant2/id1 (same id, different tenant)
	thing3 := models.NewRecordID(tableName, []any{"tenant2", "id1", models.CustomDateTime{Time: ts1}})
	_, err = surrealdb.Upsert[any](ctx, db, thing3, map[string]any{
		"tenant_id":        "tenant2",
		"_fivetran_start":  models.CustomDateTime{Time: ts1},
		"_fivetran_active": true,
		"name":             "T2-ID1",
	})
	require.NoError(t, err)

	t.Run("finds_latest_for_tenant1_id1", func(t *testing.T) {
		pkColumns := []string{"tenant_id", "id", "_fivetran_start"}
		pkValues := []any{"tenant1", "id1", models.CustomDateTime{Time: ts1}}

		rangeConfig := buildRecordIDRangeQueryBounds(pkColumns, pkValues)
		query := buildRangeQuerySubquery("name, id")

		result, err := surrealdb.Query[[]map[string]any](ctx, db, query, map[string]any{
			"tb":    tableName,
			"lower": rangeConfig.lowerBound,
			"upper": rangeConfig.upperBound,
		})
		require.NoError(t, err)
		require.Len(t, (*result)[0].Result, 1)

		record := (*result)[0].Result[0]
		assert.Equal(t, "T1-ID1 v2", record["name"], "Should get latest version of tenant1/id1")
	})

	t.Run("finds_tenant1_id2_without_including_id1", func(t *testing.T) {
		pkColumns := []string{"tenant_id", "id", "_fivetran_start"}
		pkValues := []any{"tenant1", "id2", models.CustomDateTime{Time: ts1}}

		rangeConfig := buildRecordIDRangeQueryBounds(pkColumns, pkValues)
		query := buildRangeQuerySubquery("name, id")

		result, err := surrealdb.Query[[]map[string]any](ctx, db, query, map[string]any{
			"tb":    tableName,
			"lower": rangeConfig.lowerBound,
			"upper": rangeConfig.upperBound,
		})
		require.NoError(t, err)
		require.Len(t, (*result)[0].Result, 1)

		record := (*result)[0].Result[0]
		assert.Equal(t, "T1-ID2", record["name"])
	})

	t.Run("finds_tenant2_id1_without_including_tenant1", func(t *testing.T) {
		pkColumns := []string{"tenant_id", "id", "_fivetran_start"}
		pkValues := []any{"tenant2", "id1", models.CustomDateTime{Time: ts1}}

		rangeConfig := buildRecordIDRangeQueryBounds(pkColumns, pkValues)
		query := buildRangeQuerySubquery("name, id")

		result, err := surrealdb.Query[[]map[string]any](ctx, db, query, map[string]any{
			"tb":    tableName,
			"lower": rangeConfig.lowerBound,
			"upper": rangeConfig.upperBound,
		})
		require.NoError(t, err)
		require.Len(t, (*result)[0].Result, 1)

		record := (*result)[0].Result[0]
		assert.Equal(t, "T2-ID1", record["name"])
	})
}
