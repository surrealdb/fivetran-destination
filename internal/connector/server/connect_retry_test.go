package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
)

func TestIsRetryableConnectError(t *testing.T) {
	parseErr := &url.Error{Op: "parse", URL: "://bad", Err: errors.New("invalid URI")}
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "url parse", err: parseErr, want: false},
		{name: "invalid scheme", err: errors.New("invalid connection url"), want: false},
		{name: "invalid config", err: fmt.Errorf("invalid connection config: %w", errors.New("missing host")), want: false},
		{name: "embedded", err: errors.New("embedded database not enabled"), want: false},
		{name: "connection refused", err: dialErr, want: true},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "reset", err: errors.New("read tcp: connection reset by peer"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isRetryableConnectError(tt.err))
		})
	}
}

func TestDialWithRetryStopsOnPermanentFailure(t *testing.T) {
	var calls int
	srv := newRetryTestServer(func(ctx context.Context, rawURL string) (*surrealdb.DB, error) {
		calls++
		return nil, fmt.Errorf("invalid connection url")
	})

	_, err := srv.dialWithRetry(context.Background(), "ftp://example")
	require.Error(t, err)
	require.Equal(t, 1, calls)
	require.NotContains(t, err.Error(), "after")
}

func TestDialWithRetryThenSucceeds(t *testing.T) {
	var calls int
	want := &surrealdb.DB{}
	srv := newRetryTestServer(func(ctx context.Context, rawURL string) (*surrealdb.DB, error) {
		calls++
		if calls < 3 {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		}
		return want, nil
	})

	got, err := srv.dialWithRetry(context.Background(), "ws://localhost:8000/rpc")
	require.NoError(t, err)
	require.Same(t, want, got)
	require.Equal(t, 3, calls)
}

func TestDialWithRetryExhaustedIsUserFacing(t *testing.T) {
	var calls int
	srv := newRetryTestServer(func(ctx context.Context, rawURL string) (*surrealdb.DB, error) {
		calls++
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	})

	_, err := srv.dialWithRetry(context.Background(), "ws://db.example/rpc")
	require.Error(t, err)
	require.Equal(t, 3, calls)
	require.Contains(t, err.Error(), "could not reach SurrealDB at ws://db.example/rpc after 3 attempts")
	require.Contains(t, err.Error(), "connection refused")
}

func TestDialWithRetryRespectsParentCancel(t *testing.T) {
	srv := newRetryTestServer(func(ctx context.Context, rawURL string) (*surrealdb.DB, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := srv.dialWithRetry(ctx, "ws://db.example/rpc")
	require.ErrorIs(t, err, context.Canceled)
}

func TestTaskForTokenAndGeneric(t *testing.T) {
	tokenTask := taskFor(fmt.Errorf("connect: %w", ErrTokenExpired))
	require.Contains(t, tokenTask.Message, "DEFINE USER")

	generic := taskFor(errors.New("permission denied"))
	require.Equal(t, "permission denied", generic.Message)
}

func TestMigrateResult(t *testing.T) {
	srv := New(zerolog.Nop())

	ok, err := srv.migrateResult(nil)
	require.NoError(t, err)
	_, isSuccess := ok.Response.(*pb.MigrateResponse_Success)
	require.True(t, isSuccess)

	unsupported, err := srv.migrateResult(fmt.Errorf("%w: drop", errUnsupportedMigration))
	require.NoError(t, err)
	_, isUnsupported := unsupported.Response.(*pb.MigrateResponse_Unsupported)
	require.True(t, isUnsupported)

	failed, err := srv.migrateResult(errors.New("permission denied"))
	require.NoError(t, err)
	task, isTask := failed.Response.(*pb.MigrateResponse_Task)
	require.True(t, isTask)
	require.Contains(t, task.Task.Message, "migration failed")
	require.Contains(t, task.Task.Message, "permission denied")

	expired, err := srv.migrateResult(fmt.Errorf("connect: %w", ErrTokenExpired))
	require.NoError(t, err)
	tokenTask, isTokenTask := expired.Response.(*pb.MigrateResponse_Task)
	require.True(t, isTokenTask)
	require.Contains(t, tokenTask.Task.Message, "DEFINE USER")
}

func TestMigrateMissingConfigReturnsTask(t *testing.T) {
	srv := New(zerolog.Nop())
	resp, err := srv.Migrate(context.Background(), &pb.MigrateRequest{
		Configuration: map[string]string{},
		Details:       &pb.MigrationDetails{},
	})
	require.NoError(t, err)
	task, ok := resp.Response.(*pb.MigrateResponse_Task)
	require.True(t, ok)
	require.Contains(t, task.Task.Message, "migration failed")
	require.Contains(t, task.Task.Message, "token")
}

func TestUnknownMigrationOperationIsUnsupported(t *testing.T) {
	srv := New(zerolog.Nop())
	err := srv.migrateDrop(context.Background(), nil, "ns", "table", &pb.DropOperation{})
	require.ErrorIs(t, err, errUnsupportedMigration)

	resp, rpcErr := srv.migrateResult(err)
	require.NoError(t, rpcErr)
	_, ok := resp.Response.(*pb.MigrateResponse_Unsupported)
	require.True(t, ok)
}

func newRetryTestServer(dial func(context.Context, string) (*surrealdb.DB, error)) *Server {
	srv := New(zerolog.Nop())
	srv.connectAttempts = 3
	srv.retryBaseDelay = 0
	srv.connectAttemptTimeout = time.Second
	srv.dial = dial
	return srv
}
