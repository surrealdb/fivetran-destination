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

	// Listen on all interfaces (0.0.0.0) so the server is accessible from Docker containers
	// using host.docker.internal
	addr := fmt.Sprintf("0.0.0.0:%d", port)
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
		if err := ts.Listener.Close(); err != nil {
			t.Logf("Failed to close listener: %v", err)
		}
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
		if err := conn.Close(); err != nil {
			t.Logf("Failed to close gRPC client connection: %v", err)
		}
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
			if err := conn.Close(); err != nil {
				return fmt.Errorf("failed to close gRPC client connection: %w", err)
			}

			if err == nil {
				return nil // Server is ready
			}
		}
	}
}
