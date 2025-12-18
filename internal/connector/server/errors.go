package server

import (
	"errors"
	"strings"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// ErrTokenExpired is a sentinel error for expired token authentication failures
var ErrTokenExpired = errors.New("authentication token has expired")

// isTokenExpiredError checks if an error message indicates token expiration
func isTokenExpiredError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "The token has expired")
}

// TokenExpiredTaskMessage returns the guidance message for users with expired tokens
func TokenExpiredTaskMessage() string {
	return `Your SurrealDB authentication token has expired.

To fix this, please switch to namespace-level user/password authentication:

1. Connect to your SurrealDB instance as root
2. Create a namespace-level user:
   USE NS your_namespace;
   DEFINE USER fivetran ON NAMESPACE PASSWORD "your_secure_password" ROLES OWNER;
3. In your Fivetran connector configuration:
   - Clear the "Token" field
   - Set "Authentication Level" to "namespace"
   - Set "User" to "fivetran"
   - Set "Password" to your chosen password
4. Re-test the connection

Unlike tokens, user credentials don't expire, so your Fivetran syncs will continue running without interruption.`
}

// NewTokenExpiredTask creates a Task proto message for token expiration
func NewTokenExpiredTask() *pb.Task {
	return &pb.Task{
		Message: TokenExpiredTaskMessage(),
	}
}
