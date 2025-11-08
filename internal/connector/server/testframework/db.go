package testframework

import (
	"context"
	"fmt"
	"os"
	"testing"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// GetSurrealDBEndpoint returns the SurrealDB endpoint URL from environment variable
// or defaults to ws://localhost:8000/rpc
func GetSurrealDBEndpoint() string {
	if endpoint := os.Getenv("SURREALDB_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	return "ws://localhost:8000/rpc"
}

// SetupRootConnection creates a connection to SurrealDB and authenticates as root user
// This is used for test setup operations like creating test users
func SetupRootConnection(t *testing.T, endpoint string) (*surrealdb.DB, error) {
	db, err := surrealdb.FromEndpointURLString(t.Context(), endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
	}

	_, err = db.SignIn(t.Context(), &surrealdb.Auth{
		Username: "root",
		Password: "root",
	})
	if err != nil {
		if err := db.Close(t.Context()); err != nil {
			t.Logf("failed to close SurrealDB connection: %v", err)
		}
		return nil, fmt.Errorf("failed to sign in as root: %w", err)
	}

	return db, nil
}

// CreateNamespaceLevelUser creates a namespace-level user in SurrealDB
// It removes any existing user with the same name before creating the new one
func CreateNamespaceLevelUser(t *testing.T, db *surrealdb.DB, namespace, username, password string) error {
	// Remove existing user first
	_, err := surrealdb.Query[any](t.Context(), db,
		fmt.Sprintf(`USE NS %s; REMOVE USER IF EXISTS %s ON NAMESPACE;`, namespace, username),
		nil)
	if err != nil {
		return fmt.Errorf("failed to remove existing user: %w", err)
	}

	// Create new namespace-level user
	_, err = surrealdb.Query[any](t.Context(), db,
		fmt.Sprintf(`USE NS %s; DEFINE USER %s ON NAMESPACE PASSWORD "%s" ROLES OWNER;`,
			namespace, username, password),
		nil)
	if err != nil {
		return fmt.Errorf("failed to create namespace user: %w", err)
	}

	return nil
}

// GenerateToken generates an authentication token by signing in with the provided credentials
func GenerateToken(t *testing.T, endpoint, username, password string) (string, error) {
	db, err := surrealdb.FromEndpointURLString(t.Context(), endpoint)
	if err != nil {
		return "", fmt.Errorf("failed to connect: %w", err)
	}
	defer func() {
		if err := db.Close(t.Context()); err != nil {
			t.Logf("failed to close SurrealDB connection: %v", err)
		}
	}()

	token, err := db.SignIn(t.Context(), &surrealdb.Auth{
		Username: username,
		Password: password,
	})
	if err != nil {
		return "", fmt.Errorf("failed to sign in: %w", err)
	}

	return token, nil
}

// ConnectAndUse creates a connection and selects the namespace and database
// Caller is responsible for closing the connection
func ConnectAndUse(ctx context.Context, endpoint, namespace, database, username, password string) (*surrealdb.DB, error) {
	db, err := surrealdb.FromEndpointURLString(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
	}

	_, err = db.SignIn(ctx, &surrealdb.Auth{
		Username: username,
		Password: password,
	})
	if err != nil {
		db.Close(ctx)
		return nil, fmt.Errorf("failed to sign in: %w", err)
	}

	err = db.Use(ctx, namespace, database)
	if err != nil {
		db.Close(ctx)
		return nil, fmt.Errorf("failed to use namespace/database: %w", err)
	}

	return db, nil
}
