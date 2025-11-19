package server

import (
	"context"

	"github.com/surrealdb/fivetran-destination/internal/connector/tablemapper"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// ErrTableNotFound is returned when a table is not found.
var ErrTableNotFound = tablemapper.ErrTableNotFound

func (s *Server) infoForTable(ctx context.Context, schemaName string, tableName string, configuration map[string]string) (tablemapper.TableInfo, error) {
	cfg, err := s.parseConfig(configuration)
	if err != nil {
		return tablemapper.TableInfo{}, err
	}

	db, err := s.connectAndUse(ctx, cfg, schemaName)
	if err != nil {
		return tablemapper.TableInfo{}, err
	}
	defer func() {
		if err := db.Close(ctx); err != nil {
			s.LogWarning("failed to close db", err)
		}
	}()

	tm := tablemapper.New(db, s.Logging)
	return tm.InfoForTable(ctx, tableName)
}

func (s *Server) columnsFromSurrealToFivetran(sColumns []tablemapper.ColumnInfo) ([]*pb.Column, error) {
	return tablemapper.ColumnsFromSurrealToFivetran(sColumns)
}
