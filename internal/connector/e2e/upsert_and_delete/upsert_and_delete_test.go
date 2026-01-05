package upsert_and_delete

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"

	"github.com/surrealdb/fivetran-destination/internal/connector/e2e"
	"github.com/surrealdb/fivetran-destination/internal/connector/server/testframework"
)

const (
	testNamespace = "e2e_upsert_delete_ns"
	testDatabase  = "tester"
	testPort      = 50056
)

// TestConnector_upsert_and_delete_usingSDKTester runs the Fivetran SDK tester against the connector
// and then verifies the data in SurrealDB, especially focusing on history mode behavior.
//
// This test specifically verifies that when a record is deleted in history mode:
// - The latest history record for the deleted record has _fivetran_active=false
//
// PREREQUISITES:
//  1. SurrealDB must be running at localhost:8000 with root/root credentials
//     Example: surreal start --user root --pass root --log trace
//  2. Docker must be installed and running (SDK tester runs in a container)
//
// The test:
//  1. Sets up a clean namespace in SurrealDB
//  2. Starts the connector server on port 50056 (accessible from Docker via host.docker.internal)
//  3. Runs the Fivetran SDK tester which validates the connector implementation
//  4. Verifies the SurrealDB state after SDK tester run, specifically:
//     - User id=2 should have _fivetran_active=false (deleted)
//     - User id=3 should have two history records (original + update)
//     - Other users should have _fivetran_active=true
func TestConnector_upsert_and_delete_usingSDKTester(t *testing.T) {
	// Setup clean test database
	_, err := testframework.SetupTestDB(t, testNamespace, testDatabase)
	require.NoError(t, err, "Failed to set up test database")

	// Start connector server on port 50056 (matches run_sdktester.sh)
	server := e2e.StartTestServerOnPort(t, testPort)
	defer server.Stop(t)

	// Wait for server to be ready
	ctx := context.Background()
	err = server.WaitForReady(ctx, 10*time.Second)
	require.NoError(t, err, "server should become ready")

	t.Logf("Connector server is ready on port %d", testPort)

	// Execute run_sdktester.sh (located at ./testdata/run_sdktester.sh)
	sdkTesterScript := filepath.Join("testdata", "run_sdktester.sh")

	// Verify script exists
	_, err = os.Stat(sdkTesterScript)
	require.NoError(t, err, "run_sdktester.sh should exist at %s", sdkTesterScript)

	t.Logf("Executing SDK tester: %s", sdkTesterScript)

	// Run the SDK tester script
	cmd := exec.Command("bash", sdkTesterScript)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		t.Logf("SDK tester failed (this may indicate connector bugs): %v", err)
		t.Logf("See output above for details on what failed")
		t.FailNow()
	}

	t.Log("SDK tester completed successfully - now verifying SurrealDB state")

	// Verify SurrealDB state
	verifyHistoryModeData(t)
}

// verifyHistoryModeData connects to SurrealDB and verifies the expected data state
// after the SDK tester has completed.
func verifyHistoryModeData(t *testing.T) {
	ctx := context.Background()
	endpoint := testframework.GetSurrealDBEndpoint()

	db, err := testframework.ConnectAndUse(ctx, endpoint, testNamespace, testDatabase, "root", "root")
	require.NoError(t, err, "Failed to connect to SurrealDB")
	defer func() {
		if err := db.Close(ctx); err != nil {
			t.Logf("Failed to close database: %v", err)
		}
	}()

	// Query all records from the users table
	result, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM users ORDER BY id, _fivetran_start;", nil)
	require.NoError(t, err, "Failed to query users table")
	require.NotNil(t, result, "Query result should not be nil")
	require.Len(t, *result, 1, "Expected 1 query result set")

	records := (*result)[0].Result
	t.Logf("Found %d total records in users table (including history)", len(records))

	// Log all records for debugging
	for i, record := range records {
		t.Logf("Record %d: %+v", i, record)
	}

	// Group records by user id to understand history
	recordsByID := groupRecordsByID(records)

	// Verify user id=2 (deleted user)
	// The latest history record for user id=2 should have _fivetran_active=false
	t.Run("verify_deleted_user_id_2", func(t *testing.T) {
		userRecords, exists := recordsByID[2]
		require.True(t, exists, "User id=2 should exist in the database")
		require.NotEmpty(t, userRecords, "User id=2 should have at least one history record")

		// User id=2 was only inserted once and then deleted, so it should have exactly 1 history record
		assert.Len(t, userRecords, 1, "User id=2 should have exactly 1 history record (inserted once, then deleted)")

		// Find the latest record (highest _fivetran_start)
		latestRecord := findLatestRecord(userRecords)
		require.NotNil(t, latestRecord, "Should find the latest record for user id=2")

		// The deleted record's latest history should have _fivetran_active=false
		fivetranActive, ok := latestRecord["_fivetran_active"]
		require.True(t, ok, "Record should have _fivetran_active field")

		activeBool, ok := fivetranActive.(bool)
		require.True(t, ok, "fivetran_active should be a boolean, got %T: %v", fivetranActive, fivetranActive)
		assert.False(t, activeBool, "User id=2 was deleted, so the latest history record should have _fivetran_active=false")

		t.Logf("User id=2 has %d history record(s), latest _fivetran_active=%v (expected: 1 record, false)", len(userRecords), activeBool)
	})

	// Verify user id=3 (updated user)
	// Should have 2 history records: original and updated
	t.Run("verify_updated_user_id_3", func(t *testing.T) {
		userRecords, exists := recordsByID[3]
		require.True(t, exists, "User id=3 should exist in the database")
		assert.GreaterOrEqual(t, len(userRecords), 2, "User id=3 should have at least 2 history records (original + update)")

		// The latest record should be active
		latestRecord := findLatestRecord(userRecords)
		require.NotNil(t, latestRecord, "Should find the latest record for user id=3")

		fivetranActive, ok := latestRecord["_fivetran_active"]
		require.True(t, ok, "Record should have _fivetran_active field")

		activeBool, ok := fivetranActive.(bool)
		require.True(t, ok, "fivetran_active should be a boolean, got %T: %v", fivetranActive, fivetranActive)
		assert.True(t, activeBool, "User id=3 latest record should have _fivetran_active=true")

		// The latest record should have status="Done" (from the update)
		status, ok := latestRecord["status"]
		if ok {
			assert.Equal(t, "Done", status, "User id=3 latest record should have status='Done' after update")
		}

		t.Logf("User id=3 has %d history records, latest _fivetran_active=%v", len(userRecords), activeBool)
	})

	// Verify other users (1, 4, 5, 6, 7) should have _fivetran_active=true
	t.Run("verify_active_users", func(t *testing.T) {
		activeUserIDs := []int{1, 4, 5, 6, 7}
		for _, userID := range activeUserIDs {
			userRecords, exists := recordsByID[userID]
			if !exists {
				t.Logf("Warning: User id=%d not found in database", userID)
				continue
			}

			latestRecord := findLatestRecord(userRecords)
			require.NotNil(t, latestRecord, "Should find the latest record for user id=%d", userID)

			fivetranActive, ok := latestRecord["_fivetran_active"]
			require.True(t, ok, "Record for user id=%d should have _fivetran_active field", userID)

			activeBool, ok := fivetranActive.(bool)
			require.True(t, ok, "fivetran_active for user id=%d should be a boolean, got %T: %v", userID, fivetranActive, fivetranActive)
			assert.True(t, activeBool, "User id=%d should have _fivetran_active=true", userID)
		}
	})

	// Verify total expected users exist
	t.Run("verify_expected_users_exist", func(t *testing.T) {
		expectedUserIDs := []int{1, 2, 3, 4, 5, 6, 7}
		for _, userID := range expectedUserIDs {
			_, exists := recordsByID[userID]
			assert.True(t, exists, "User id=%d should exist in the database", userID)
		}
	})
}

// groupRecordsByID groups history records by their id field
func groupRecordsByID(records []map[string]any) map[int][]map[string]any {
	result := make(map[int][]map[string]any)

	for _, record := range records {
		id, ok := extractID(record)
		if !ok {
			continue
		}
		result[id] = append(result[id], record)
	}

	return result
}

// extractID extracts the integer ID from a record
func extractID(record map[string]any) (int, bool) {
	idVal, ok := record["id"]
	if !ok {
		return 0, false
	}

	// SurrealDB can return id in various formats
	switch v := idVal.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case models.RecordID:
		// In history mode, SurrealDB RecordID.ID is an array like [user_id, _fivetran_start]
		return extractIDFromRecordID(v)
	case *models.RecordID:
		// Pointer to RecordID
		return extractIDFromRecordID(*v)
	case map[string]any:
		// RecordID structure: may have "id" field with array
		if idArr, ok := v["id"].([]any); ok && len(idArr) > 0 {
			return extractIntFromAny(idArr[0])
		}
	}

	return 0, false
}

// extractIDFromRecordID extracts the user ID from a SurrealDB RecordID
func extractIDFromRecordID(rid models.RecordID) (int, bool) {
	// RecordID.ID can be various types
	switch idContent := rid.ID.(type) {
	case []any:
		if len(idContent) > 0 {
			return extractIntFromAny(idContent[0])
		}
	default:
		// Try reflection to handle slice types
		return extractIntFromSliceReflect(rid.ID)
	}
	return 0, false
}

// extractIntFromSliceReflect uses reflection to extract int from first element of any slice
func extractIntFromSliceReflect(v any) (int, bool) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		panic(fmt.Sprintf("extractIntFromSliceReflect: expected slice, got %v (type=%T)", rv.Kind(), v))
	}
	if rv.Len() == 0 {
		panic(fmt.Sprintf("extractIntFromSliceReflect: slice is empty (type=%T)", v))
	}
	firstElem := rv.Index(0).Interface()
	return extractIntFromAny(firstElem)
}

// extractIntFromAny extracts an int from various types
func extractIntFromAny(v any) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	case uint64:
		return int(val), true
	default:
		panic(fmt.Sprintf("extractIntFromAny: unhandled type %T, value=%v", v, v))
	}
}

// findLatestRecord finds the record with the highest _fivetran_start timestamp
func findLatestRecord(records []map[string]any) map[string]any {
	if len(records) == 0 {
		return nil
	}

	var latest map[string]any
	var latestTime time.Time

	for _, record := range records {
		ftStart, ok := record["_fivetran_start"]
		if !ok {
			continue
		}

		var recordTime time.Time

		switch v := ftStart.(type) {
		case time.Time:
			recordTime = v
		case models.CustomDateTime:
			recordTime = v.Time
		case string:
			parsed, err := time.Parse(time.RFC3339, v)
			if err != nil {
				parsed, err = time.Parse(time.RFC3339Nano, v)
				if err != nil {
					continue
				}
			}
			recordTime = parsed
		default:
			// Try to extract from SurrealDB CustomDateTime
			if stringer, ok := v.(fmt.Stringer); ok {
				parsed, err := time.Parse(time.RFC3339, stringer.String())
				if err != nil {
					parsed, err = time.Parse(time.RFC3339Nano, stringer.String())
					if err != nil {
						continue
					}
				}
				recordTime = parsed
			} else {
				continue
			}
		}

		if latest == nil || recordTime.After(latestTime) {
			latest = record
			latestTime = recordTime
		}
	}

	return latest
}
