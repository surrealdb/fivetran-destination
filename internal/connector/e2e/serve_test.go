package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/surrealdb/fivetran-destination/internal/connector"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// TestServer represents a running connector server for e2e testing
type TestServer struct {
	Listener   net.Listener
	GRPCServer *grpc.Server
	Port       int
	Logger     zerolog.Logger
	stopChan   chan struct{}
	errChan    chan error
}

// StartTestServerOnPort starts a connector server on the specified port
// Port must be non-zero. Use unique ports for parallel tests to avoid conflicts.
//
// Example usage for parallel tests:
//
//	func TestParallel1(t *testing.T) {
//	    t.Parallel()
//	    server := StartTestServerOnPort(t, 50001)
//	    defer server.Stop(t)
//	    // ... test code ...
//	}
//
//	func TestParallel2(t *testing.T) {
//	    t.Parallel()
//	    server := StartTestServerOnPort(t, 50002)
//	    defer server.Stop(t)
//	    // ... test code ...
//	}
func StartTestServerOnPort(t *testing.T, port int) *TestServer {
	t.Helper()

	// Require explicit port number
	require.NotEqual(t, 0, port, "port must be explicitly specified (non-zero)")

	// Create logger with debug level
	logger := zerolog.New(os.Stdout).
		Level(zerolog.DebugLevel).
		With().
		Timestamp().
		Logger()

	// Listen on specified port
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err, "failed to create listener on %s", addr)

	ts := &TestServer{
		Listener: lis,
		Port:     port,
		Logger:   logger,
		stopChan: make(chan struct{}),
		errChan:  make(chan error, 1),
	}

	// Start server in background
	go func() {
		logger.Info().Int("port", port).Msg("Starting SurrealDB destination connector for e2e test")
		if err := connector.Serve(lis, logger); err != nil {
			select {
			case ts.errChan <- err:
			case <-ts.stopChan:
				// Server was stopped intentionally
			}
		}
	}()

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	t.Logf("Test server started on port %d", port)

	return ts
}

// Stop stops the test server gracefully
func (ts *TestServer) Stop(t *testing.T) {
	t.Helper()

	close(ts.stopChan)

	if ts.GRPCServer != nil {
		ts.Logger.Info().Msg("Stopping gRPC server gracefully")
		ts.GRPCServer.GracefulStop()
	}

	if ts.Listener != nil {
		ts.Listener.Close()
	}

	// Check if there was an error during server operation
	select {
	case err := <-ts.errChan:
		if err != nil {
			t.Logf("Server error: %v", err)
		}
	default:
	}

	t.Logf("Test server stopped")
}

// NewClient creates a gRPC client connected to this test server
func (ts *TestServer) NewClient(t *testing.T) pb.DestinationConnectorClient {
	t.Helper()

	addr := fmt.Sprintf("127.0.0.1:%d", ts.Port)

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "failed to create gRPC client")

	// Register cleanup to close connection
	t.Cleanup(func() {
		conn.Close()
	})

	return pb.NewDestinationConnectorClient(conn)
}

// WaitForReady waits for the server to be ready to accept connections
// Returns error if server doesn't become ready within timeout
func (ts *TestServer) WaitForReady(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	addr := fmt.Sprintf("127.0.0.1:%d", ts.Port)

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("server not ready within timeout: %w", ctx.Err())
		case err := <-ts.errChan:
			return fmt.Errorf("server error: %w", err)
		case <-ticker.C:
			// Try to connect
			conn, err := grpc.NewClient(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				continue
			}

			// Try a simple RPC call
			client := pb.NewDestinationConnectorClient(conn)
			_, err = client.ConfigurationForm(ctx, &pb.ConfigurationFormRequest{})
			conn.Close()

			if err == nil {
				return nil // Server is ready
			}
		}
	}
}

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
