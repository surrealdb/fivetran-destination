package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// Example test demonstrating usage
func TestServerStartStop(t *testing.T) {
	// Start test server on port 50001
	server := StartTestServerOnPort(t, 50001)
	defer server.Stop(t)

	// Wait for server to be ready
	ctx := context.Background()
	err := server.WaitForReady(ctx, 5*time.Second)
	require.NoError(t, err, "server should become ready")

	// Create client
	client := server.NewClient(t)

	// Make a test RPC call
	resp, err := client.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
	require.NoError(t, err, "ConfigurationForm should succeed")
	require.NotNil(t, resp, "response should not be nil")

	t.Logf("ConfigurationForm response: %+v", resp)
}

// TestServerMultipleClients tests that multiple clients can connect to the same server
func TestServerMultipleClients(t *testing.T) {
	server := StartTestServerOnPort(t, 50002)
	defer server.Stop(t)

	ctx := context.Background()
	err := server.WaitForReady(ctx, 5*time.Second)
	require.NoError(t, err)

	// Create multiple clients
	client1 := server.NewClient(t)
	client2 := server.NewClient(t)
	client3 := server.NewClient(t)

	// All clients should be able to make RPC calls
	_, err = client1.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
	require.NoError(t, err)

	_, err = client2.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
	require.NoError(t, err)

	_, err = client3.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
	require.NoError(t, err)
}

// TestServerParallelTests demonstrates running parallel tests with different ports
func TestServerParallelTests(t *testing.T) {
	t.Run("Server1", func(t *testing.T) {
		t.Parallel()
		server := StartTestServerOnPort(t, 50011)
		defer server.Stop(t)

		ctx := context.Background()
		err := server.WaitForReady(ctx, 5*time.Second)
		require.NoError(t, err)

		client := server.NewClient(t)
		_, err = client.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
		require.NoError(t, err)
	})

	t.Run("Server2", func(t *testing.T) {
		t.Parallel()
		server := StartTestServerOnPort(t, 50012)
		defer server.Stop(t)

		ctx := context.Background()
		err := server.WaitForReady(ctx, 5*time.Second)
		require.NoError(t, err)

		client := server.NewClient(t)
		_, err = client.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
		require.NoError(t, err)
	})

	t.Run("Server3", func(t *testing.T) {
		t.Parallel()
		server := StartTestServerOnPort(t, 50013)
		defer server.Stop(t)

		ctx := context.Background()
		err := server.WaitForReady(ctx, 5*time.Second)
		require.NoError(t, err)

		client := server.NewClient(t)
		_, err = client.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
		require.NoError(t, err)
	})
}
