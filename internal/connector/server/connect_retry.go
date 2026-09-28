package server

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/surrealdb/surrealdb.go"
)

const (
	defaultConnectAttempts       = 3
	defaultConnectAttemptTimeout = 5 * time.Second
	defaultConnectRetryDelay     = 200 * time.Millisecond
)

type connectSettings struct {
	attempts       int
	attemptTimeout time.Duration
	retryDelay     time.Duration
}

func (s *Server) effectiveConnectConfig() connectSettings {
	if s.connectAttempts <= 0 {
		return connectSettings{
			attempts:       defaultConnectAttempts,
			attemptTimeout: defaultConnectAttemptTimeout,
			retryDelay:     defaultConnectRetryDelay,
		}
	}
	timeout := s.connectAttemptTimeout
	if timeout <= 0 {
		timeout = defaultConnectAttemptTimeout
	}
	return connectSettings{
		attempts:       s.connectAttempts,
		attemptTimeout: timeout,
		retryDelay:     s.retryBaseDelay,
	}
}

func (s *Server) dialEndpoint(ctx context.Context, rawURL string) (*surrealdb.DB, error) {
	if s.dial != nil {
		return s.dial(ctx, rawURL)
	}
	return surrealdb.FromEndpointURLString(ctx, rawURL)
}

// dialWithRetry dials SurrealDB. A failure that happens before the server answers
// (refused, timeout, reset) is retried. A bad URL or scheme is returned immediately.
// Auth and query errors are not handled here; those happen after a connection exists.
func (s *Server) dialWithRetry(ctx context.Context, rawURL string) (*surrealdb.DB, error) {
	cfg := s.effectiveConnectConfig()
	var last error
	for attempt := 1; attempt <= cfg.attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
		}

		attemptCtx, cancel := context.WithTimeout(ctx, cfg.attemptTimeout)
		db, err := s.dialEndpoint(attemptCtx, rawURL)
		cancel()
		if err == nil {
			return db, nil
		}
		last = err
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to connect to SurrealDB: %w", ctx.Err())
		}
		if !isRetryableConnectError(err) {
			return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
		}
		if attempt == cfg.attempts {
			break
		}
		s.LogWarning("retrying connection to SurrealDB", err, "attempt", attempt, "url", rawURL)
		if err := sleepCtx(ctx, cfg.retryDelay*time.Duration(attempt)); err != nil {
			return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
		}
	}
	return nil, fmt.Errorf("could not reach SurrealDB at %s after %d attempts. Check that the URL is correct, the instance is running, and Fivetran can reach it. Last error: %w", rawURL, cfg.attempts, last)
}

// isRetryableConnectError reports whether a dial failure might succeed on another attempt.
// URL parsing and scheme errors are permanent. Everything else from the dial is ambiguous
// transport failure and is retried.
func isRetryableConnectError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var parseErr *url.Error
	if errors.As(err, &parseErr) && parseErr.Op == "parse" {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "invalid connection config"),
		strings.Contains(msg, "invalid connection url"),
		strings.Contains(msg, "embedded database not enabled"):
		return false
	default:
		return true
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
