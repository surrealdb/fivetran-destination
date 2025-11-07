package connector

import (
	"context"
	"fmt"
	"time"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
)

func (s *Server) truncate(ctx context.Context, req *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	if s.debugging() {
		s.logDebug("Truncate called",
			"schema", req.SchemaName,
			"table", req.TableName,
			// SyncedColumn is e.g. `_sivetran_synced` which is timestamp-like column/field
			"syncedColumn", req.SyncedColumn,
		)
		if req.Soft != nil {
			s.logDebug("This is a soft truncation. See https://github.com/fivetran/fivetran_partner_sdk/blob/main/development-guide.md#truncate",
				"soft.deletedColumn", req.Soft.DeletedColumn,
			)
		}
		if req.UtcDeleteBefore != nil {
			s.logDebug("UtcDeleteBefore", "time", req.UtcDeleteBefore.AsTime().Format(time.RFC3339))
		}

		// You usually do something like:
		//   SOFT DELETE:  `UPDATE <table> SET _fivetran_deleted = true WHERE _fivetran_synced <= <UtcDeleteBefore>`
		//   HARD DELETE:  `DELETE FROM <table> WHERE _fivetran_synced <= <UtcDeleteBefore>`
	}
	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.TruncateResponse{
			Response: &pb.TruncateResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	db, err := s.connect(ctx, cfg, req.SchemaName)
	if err != nil {
		return &pb.TruncateResponse{
			Response: &pb.TruncateResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	defer func() {
		if err := db.Close(ctx); err != nil {
			s.logWarning("failed to close db", err)
		}
	}()

	if req.Soft != nil {
		// DeletedColumn is e.g. `_sivetran_deleted` which is bool-like column/field
		s.logInfo("Doing a soft truncation",
			"soft.deletedColumn", req.Soft.DeletedColumn,
		)

		if err := s.softTruncate(ctx, db, req); err != nil {
			return &pb.TruncateResponse{
				Response: &pb.TruncateResponse_Warning{
					Warning: &pb.Warning{
						Message: err.Error(),
					},
				},
			}, err
		}
	}

	return &pb.TruncateResponse{
		Response: &pb.TruncateResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) softTruncate(ctx context.Context, db *surrealdb.DB, req *pb.TruncateRequest) error {
	deletedColumn := req.Soft.DeletedColumn

	res, err := surrealdb.Query[any](ctx, db, "UPDATE type::table($tb) SET "+deletedColumn+" = true WHERE type::field($sc) <= type::datetime($utc)", map[string]interface{}{
		"tb":  req.TableName,
		"dc":  deletedColumn,
		"sc":  req.SyncedColumn,
		"utc": req.UtcDeleteBefore.AsTime().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("failed to soft truncate: %w", err)
	}

	if s.debugging() {
		s.logDebug("SoftTruncate result", "result", res)
	}

	return nil
}
