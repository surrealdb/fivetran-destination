package basic

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/surrealdb/fivetran-destination/internal/connector/e2e"
	"github.com/surrealdb/fivetran-destination/internal/connector/server/testframework"
)

// TestConnector_sync_modes_usingSDKTester runs the Fivetran SDK tester against the connector.
//
// PREREQUISITES:
//  1. SurrealDB must be running at localhost:8000 with root/root credentials
//     Example: surreal start --user root --pass root --log trace
//  2. Docker must be installed and running (SDK tester runs in a container)
//
// The test:
//  1. Starts the connector server on port 50055 (accessible from Docker via host.docker.internal)
//  2. Runs the Fivetran SDK tester which validates the connector implementation
//  3. Uses configuration from testdata/configuration.json and testdata/input.json
//
// Expected duration: ~10-15 seconds
// The test will fail if the SDK tester finds bugs in the connector implementation.
func TestConnector_sync_modes_usingSDKTester(t *testing.T) {
	// Start connector server on port 50055 (matches run_sdktester.sh)
	server := e2e.StartTestServerOnPort(t, 50055)
	defer server.Stop(t)

	_, err := testframework.SetupTestDB(t, "e2e_sync_modes_ns", "tester")
	require.NoError(t, err, "Failed to set up test database")

	// Wait for server to be ready
	ctx := context.Background()
	err = server.WaitForReady(ctx, 10*time.Second)
	require.NoError(t, err, "server should become ready")

	t.Log("Connector server is ready on port 50055")

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
	// Note: SDK tester may fail if it finds bugs in the connector implementation
	// For now, we just log whether it passed or failed
	if err != nil {
		t.Logf("SDK tester failed (this may indicate connector bugs): %v", err)
		t.Logf("See output above for details on what failed")
		t.FailNow()
	}

	t.Log("SDK tester completed successfully - all Fivetran requirements validated!")

	// TODO: Add assertions to verify SurrealDB state after SDK tester run
	// This would include:
	// - Connect to SurrealDB at localhost:8000 with namespace e2e_basic_ns
	// - Verify expected tables exist (transaction, campaign, composite_table)
	// - Verify expected records were created/updated/deleted according to input.json
	// - Verify data integrity:
	//   * Check _fivetran_synced timestamps
	//   * For history mode tables: verify _fivetran_start, _fivetran_end, _fivetran_active
	//   * Verify primary key constraints
	//   * Verify data types match schema
	// - Test soft deletes and upserts worked correctly
	// - Verify composite primary keys (pk1, pk2) in composite_table
	// - Clean up test data:
	//   * Drop tables: transaction, campaign, composite_table
	//   * Or drop the entire namespace: e2e_basic_ns
}
