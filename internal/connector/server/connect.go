package server

import (
	"context"
	"fmt"

	"github.com/surrealdb/surrealdb.go"
)

// connect connects to SurrealDB and returns a DB instance
//
// It authenticates against the SurrealDB instance as a namespace-level user
// with the SurrealDB namespace specified in cfg.ns (via ConfigurationForm).
//
// Depending on whether cfg.token is set, it either uses the provided token for authentication,
// or performs a sign-in using the provided username and password.
//
// The caller is responsible for "Use"ing ns/db after calling this function
// Use connectAndUse if you want to connect and use a specific database right away.
func (s *Server) connect(ctx context.Context, cfg config) (*surrealdb.DB, error) {
	db, err := surrealdb.FromEndpointURLString(ctx, cfg.url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
	}

	token := cfg.token

	if token == "" {
		if err := s.signIn(ctx, db, cfg); err != nil {
			return nil, fmt.Errorf("failed to sign in to SurrealDB: %w", err)
		}
		return db, nil
	}

	// If you end up panicking here like `panic: cbor: 18 bytes of extraneous data starting at index 21`,
	// Use the WebSocket endpoint instead of the HTTP endpoint.
	// See https://github.com/surrealdb/surrealdb.go/pull/201
	//
	// Just for anyone reading this, by HTTP and WebSocket endpoints, I mean `http://localhost:8000/rpc` and `ws://localhost:8000/rpc`
	// respectively.
	if err := db.Authenticate(ctx, token); err != nil {
		return nil, fmt.Errorf("failed to authenticate with SurrealDB: %w", err)
	}

	return db, nil
}

func (s *Server) signIn(ctx context.Context, db *surrealdb.DB, cfg config) error {
	auth := &surrealdb.Auth{
		Username: cfg.user,
		Password: cfg.pass,
	}

	var authLevelStr string
	switch cfg.authLevel {
	case AuthLevelRoot:
		authLevelStr = "root"
	case AuthLevelNamespace:
		// We set only the namespace, so that we sign in as a namespace-level user,
		// rather than a root-level or a database-level user.
		auth.Namespace = cfg.ns

		authLevelStr = "namespace"
	default:
		return fmt.Errorf("unknown auth level: %v", cfg.authLevel)
	}

	_, err := db.SignIn(ctx, auth)
	if err != nil {
		return fmt.Errorf("failed to sign in to SurrealDB as a %s-level user: %w", authLevelStr, err)
	}
	return nil
}

// connectAndUse connects to SurrealDB and returns a DB instance
//
// It authenticates against the SurrealDB instance as a namespace-level user
// with the SurrealDB namespace specified in cfg.ns (via ConfigurationForm),
// and then switches to the specified database (schema) using USE.
func (s *Server) connectAndUse(ctx context.Context, cfg config, schema string) (*surrealdb.DB, error) {
	db, err := s.connect(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Note that we assume Fivetran `schema` is the same as SurrealDB `database`.
	// So we treat SurrealDB `namespace` as a global setting, that limits every operation from this
	// connector to SurrealDB within the namespace.
	//
	// If you read this connector's implementation,
	// you'll notice Fivetran calls our RPCs like `hey, create a table named <schema>.<table>`,
	// and we interpret it as `ok let's create a table <table> in database <schema>`.
	if err := db.Use(ctx, cfg.ns, schema); err != nil {
		return nil, fmt.Errorf("failed to use namespace %s: %w", cfg.ns, err)
	}

	return db, nil
}
