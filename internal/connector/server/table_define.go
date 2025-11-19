package server

import (
	"context"

	"github.com/surrealdb/fivetran-destination/internal/connector/tablemapper"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
)

func (s *Server) defineTable(ctx context.Context, db *surrealdb.DB, table *pb.Table) error {
	tm := tablemapper.New(db, s.Logging)
	return tm.DefineTable(ctx, table)
}
