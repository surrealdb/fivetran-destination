package server

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	surrealdb "github.com/surrealdb/surrealdb.go"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// getSurrealDBEndpoint returns the SurrealDB endpoint URL from environment variable
// or defaults to ws://localhost:8000/rpc
func getSurrealDBEndpoint() string {
	if endpoint := os.Getenv("SURREALDB_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	return "ws://localhost:8000/rpc"
}

// setupRootConnection creates a connection to SurrealDB and authenticates as root user
// This is used for test setup operations like creating test users
func setupRootConnection(t *testing.T, endpoint string) (*surrealdb.DB, error) {
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

// createNamespaceLevelUser creates a namespace-level user in SurrealDB
// It removes any existing user with the same name before creating the new one
func createNamespaceLevelUser(t *testing.T, db *surrealdb.DB, namespace, username, password string) error {
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

// generateToken generates an authentication token by signing in with the provided credentials
func generateToken(t *testing.T, endpoint, username, password string) (string, error) {
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

func TestServerTest_SuccessWithRootAuth(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	resp, err := srv.Test(t.Context(), &pb.TestRequest{
		Name: "database-connection",
		Configuration: map[string]string{
			"url":        getSurrealDBEndpoint(),
			"ns":         "test",
			"user":       "root",
			"pass":       "root",
			"auth_level": "root",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	success, ok := resp.Response.(*pb.TestResponse_Success)
	require.True(t, ok, "Expected success response")
	require.True(t, success.Success)
}

func TestServerTest_SuccessWithRootAuthDefault(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	// Test without explicit auth_level - should default to root
	resp, err := srv.Test(t.Context(), &pb.TestRequest{
		Name: "database-connection",
		Configuration: map[string]string{
			"url":  getSurrealDBEndpoint(),
			"ns":   "test",
			"user": "root",
			"pass": "root",
			// auth_level not specified - defaults to root
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	success, ok := resp.Response.(*pb.TestResponse_Success)
	require.True(t, ok, "Expected success response")
	require.True(t, success.Success)
}

func TestServerTest_SuccessWithNamespaceAuth(t *testing.T) {
	ctx := context.Background()
	endpoint := getSurrealDBEndpoint()

	// Setup: Connect as root and create namespace-level test user
	db, err := setupRootConnection(t, endpoint)
	require.NoError(t, err, "Failed to connect as root for test setup")
	defer func() {
		if err := db.Close(t.Context()); err != nil {
			t.Logf("failed to close SurrealDB connection: %v", err)
		}
	}()

	err = createNamespaceLevelUser(t, db, "test", "nsuser", "nspass")
	require.NoError(t, err, "Failed to create namespace-level user")

	// Close root connection before testing
	if err := db.Close(t.Context()); err != nil {
		t.Logf("failed to close SurrealDB connection: %v", err)
	}

	// Test: Authenticate with the namespace-level user
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	resp, err := srv.Test(ctx, &pb.TestRequest{
		Name: "database-connection",
		Configuration: map[string]string{
			"url":        endpoint,
			"ns":         "test",
			"user":       "nsuser",
			"pass":       "nspass",
			"auth_level": "namespace",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	success, ok := resp.Response.(*pb.TestResponse_Success)
	require.True(t, ok, "Expected success response")
	require.True(t, success.Success)
}

func TestServerTest_SuccessWithToken(t *testing.T) {
	endpoint := getSurrealDBEndpoint()

	// Setup: Generate a token by signing in as root
	token, err := generateToken(t, endpoint, "root", "root")
	require.NoError(t, err, "Failed to generate token")
	require.NotEmpty(t, token, "Generated token should not be empty")

	// Test: Authenticate using the generated token
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	resp, err := srv.Test(t.Context(), &pb.TestRequest{
		Name: "database-connection",
		Configuration: map[string]string{
			"url":   endpoint,
			"ns":    "test",
			"token": token,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	success, ok := resp.Response.(*pb.TestResponse_Success)
	require.True(t, ok, "Expected success response")
	require.True(t, success.Success)
}

func TestServerTest_FailureInvalidCredentials(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	resp, err := srv.Test(t.Context(), &pb.TestRequest{
		Name: "database-connection",
		Configuration: map[string]string{
			"url":  getSurrealDBEndpoint(),
			"ns":   "test",
			"user": "invalid_user",
			"pass": "invalid_password",
		},
	})

	// The Test method returns both a failure response and an error
	require.Error(t, err)
	require.NotNil(t, resp)
	failure, ok := resp.Response.(*pb.TestResponse_Failure)
	require.True(t, ok, "Expected failure response for invalid credentials")
	require.NotEmpty(t, failure.Failure)
	t.Logf("Failure message: %s", failure.Failure)
}

func TestServerTest_FailureInvalidURL(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	resp, err := srv.Test(t.Context(), &pb.TestRequest{
		Name: "database-connection",
		Configuration: map[string]string{
			"url":  "ws://invalid-host:9999/rpc",
			"ns":   "test",
			"user": "root",
			"pass": "root",
		},
	})

	// The Test method returns both a failure response and an error
	require.Error(t, err)
	require.NotNil(t, resp)
	failure, ok := resp.Response.(*pb.TestResponse_Failure)
	require.True(t, ok, "Expected failure response for invalid URL")
	require.NotEmpty(t, failure.Failure)
	t.Logf("Failure message: %s", failure.Failure)
}

func TestServerTest_FailureMissingURL(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	resp, err := srv.Test(t.Context(), &pb.TestRequest{
		Name: "database-connection",
		Configuration: map[string]string{
			// url missing
			"ns":   "test",
			"user": "root",
			"pass": "root",
		},
	})

	// The Test method returns both a failure response and an error
	require.Error(t, err)
	require.NotNil(t, resp)
	failure, ok := resp.Response.(*pb.TestResponse_Failure)
	require.True(t, ok, "Expected failure response for missing URL")
	require.NotEmpty(t, failure.Failure)
	require.Contains(t, failure.Failure, "url", "Error should mention missing url")
	t.Logf("Failure message: %s", failure.Failure)
}

func TestServerTest_FailureMissingNamespace(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	resp, err := srv.Test(t.Context(), &pb.TestRequest{
		Name: "database-connection",
		Configuration: map[string]string{
			"url": getSurrealDBEndpoint(),
			// ns missing
			"user": "root",
			"pass": "root",
		},
	})

	// The Test method returns both a failure response and an error
	require.Error(t, err)
	require.NotNil(t, resp)
	failure, ok := resp.Response.(*pb.TestResponse_Failure)
	require.True(t, ok, "Expected failure response for missing namespace")
	require.NotEmpty(t, failure.Failure)
	require.Contains(t, failure.Failure, "ns", "Error should mention missing namespace")
	t.Logf("Failure message: %s", failure.Failure)
}

func TestServerTest_FailureMissingCredentials(t *testing.T) {
	srv := New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))

	resp, err := srv.Test(t.Context(), &pb.TestRequest{
		Name: "database-connection",
		Configuration: map[string]string{
			"url": getSurrealDBEndpoint(),
			"ns":  "test",
			// no user/pass or token provided
		},
	})

	// The Test method returns both a failure response and an error
	require.Error(t, err)
	require.NotNil(t, resp)
	failure, ok := resp.Response.(*pb.TestResponse_Failure)
	require.True(t, ok, "Expected failure response for missing credentials")
	require.NotEmpty(t, failure.Failure)
	t.Logf("Failure message: %s", failure.Failure)
}
