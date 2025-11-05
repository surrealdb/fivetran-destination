package connector

import (
	"context"
	"fmt"

	"github.com/surrealdb/surrealdb.go"
)

// connect connects to SurrealDB and returns a DB instance
// The caller is responsible for "Use"ing ns/db after calling this function
func (s *Server) connect(ctx context.Context, cfg config, schema string) (*surrealdb.DB, error) {
	db, err := surrealdb.New(cfg.url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
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

	token := cfg.token

	if token == "" {
		token, err = db.SignIn(ctx, &surrealdb.Auth{
			Username: cfg.user,
			Password: cfg.pass,
			// Use `Use` instead of setting `Namespace` and `Database` here.
			// Otherwise, you end up with: failed to sign in to SurrealDB: namespace or database or both are not set
			// Probably related to https://github.com/surrealdb/surrealdb.node/issues/26#issuecomment-2057102554
			// Namespace: cfg.ns,
			// Database:  cfg.db,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to sign in to SurrealDB: %w", err)
		}
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
