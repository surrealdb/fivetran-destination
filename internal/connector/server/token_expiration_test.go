package server

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsTokenExpiredError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "exact match",
			err:      errors.New("The token has expired"),
			expected: true,
		},
		{
			name:     "wrapped error",
			err:      fmt.Errorf("auth failed: %w", errors.New("The token has expired")),
			expected: true,
		},
		{
			name:     "substring match with context",
			err:      errors.New("SurrealDB error: The token has expired. Please re-authenticate."),
			expected: true,
		},
		{
			name:     "unrelated error",
			err:      errors.New("connection refused"),
			expected: false,
		},
		{
			name:     "similar but different - lowercase",
			err:      errors.New("the token has expired"),
			expected: false,
		},
		{
			name:     "similar but different - no 'The'",
			err:      errors.New("token has expired"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTokenExpiredError(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestErrTokenExpiredSentinel(t *testing.T) {
	t.Run("wrapped error can be detected with errors.Is", func(t *testing.T) {
		wrappedErr := fmt.Errorf("%w: original error", ErrTokenExpired)
		require.True(t, errors.Is(wrappedErr, ErrTokenExpired))
	})

	t.Run("double-wrapped error can be detected", func(t *testing.T) {
		wrappedErr := fmt.Errorf("%w: original error", ErrTokenExpired)
		doubleWrapped := fmt.Errorf("connection failed: %w", wrappedErr)
		require.True(t, errors.Is(doubleWrapped, ErrTokenExpired))
	})

	t.Run("unrelated error is not detected as token expired", func(t *testing.T) {
		unrelatedErr := errors.New("some other error")
		require.False(t, errors.Is(unrelatedErr, ErrTokenExpired))
	})
}

func TestNewTokenExpiredTask(t *testing.T) {
	task := NewTokenExpiredTask()
	require.NotNil(t, task)
	require.NotEmpty(t, task.Message)

	// Verify key guidance elements are present in the message
	require.Contains(t, task.Message, "token has expired")
	require.Contains(t, task.Message, "DEFINE USER")
	require.Contains(t, task.Message, "namespace")
	require.Contains(t, task.Message, "Authentication Level")
}

func TestTokenExpiredTaskMessage(t *testing.T) {
	msg := TokenExpiredTaskMessage()
	require.NotEmpty(t, msg)

	// Verify the message contains actionable guidance
	require.Contains(t, msg, "namespace-level user/password authentication")
	require.Contains(t, msg, "DEFINE USER")
	require.Contains(t, msg, "ROLES OWNER")
	require.Contains(t, msg, "Re-test the connection")

	// Verify the user-friendly explanation is present
	require.Contains(t, msg, "user credentials don't expire")
}
