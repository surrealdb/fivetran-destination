package migrator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"

	"github.com/surrealdb/fivetran-destination/internal/connector/log"
	"github.com/surrealdb/fivetran-destination/internal/connector/server/testframework"
)

// testNamespace returns a unique namespace based on the test name
func testNamespace(t *testing.T) string {
	// Replace invalid characters with underscores
	name := strings.ReplaceAll(t.Name(), "/", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return fmt.Sprintf("test_%s", name)
}

// testSetup creates a connection to SurrealDB for testing
// It cleans up any existing data in the namespace and returns a configured DB and Migrator
// The DB connection is automatically closed when the test completes via t.Cleanup
func testSetup(t *testing.T, namespace string) (*surrealdb.DB, *Migrator) {
	db, err := testframework.SetupTestDB(t, namespace, "testdb")
	require.NoError(t, err, "Failed to setup test database")

	// Register cleanup to close the database connection
	t.Cleanup(func() {
		if err := db.Close(t.Context()); err != nil {
			t.Logf("Failed to close db: %v", err)
		}
	})

	// Create logger and migrator
	logger := testframework.GetTestLogger()
	logging := &log.Logging{Logger: logger}
	migrator := New(db, logging)

	return db, migrator
}
