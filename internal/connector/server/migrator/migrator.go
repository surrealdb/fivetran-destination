package migrator

import (
	"github.com/surrealdb/fivetran-destination/internal/connector/log"
	"github.com/surrealdb/surrealdb.go"
)

type Migrator struct {
	db *surrealdb.DB

	*log.Logging
}

func New(db *surrealdb.DB, logger *log.Logging) *Migrator {
	return &Migrator{
		db:      db,
		Logging: logger,
	}
}
