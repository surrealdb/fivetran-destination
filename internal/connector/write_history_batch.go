package connector

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

func (s *Server) writeHistoryBatch(ctx context.Context, req *pb.WriteHistoryBatchRequest) (*pb.WriteBatchResponse, error) {
	if s.Debugging() {
		s.LogDebug("WriteHistoryBatch called", "schema", req.SchemaName, "table", req.Table.Name)
		s.LogDebug("Earliest start files", "count", len(req.EarliestStartFiles))
		s.LogDebug("Replace files", "count", len(req.ReplaceFiles))
		s.LogDebug("Update files", "count", len(req.UpdateFiles))
		s.LogDebug("Delete files", "count", len(req.DeleteFiles))
		s.LogDebug("FileParams",
			"compression", req.FileParams.Compression,
			"encryption", req.FileParams.Encryption,
			"null_string", req.FileParams.NullString,
			"unmodified_string", req.FileParams.UnmodifiedString)
	}

	cfg, err := s.parseConfig(req.Configuration)
	if err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: fmt.Sprintf("failed parsing write history batch config: %v", err.Error()),
				},
			},
		}, err
	}

	db, err := s.connect(ctx, cfg, req.SchemaName)
	if err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}
	defer func() {
		if err := db.Close(ctx); err != nil {
			s.LogWarning("failed to close db", err)
		}
	}()

	if s.Debugging() {
		s.LogDebug("WriteHistoryBatch using", "namespace", cfg.ns, "database", req.SchemaName)
	}

	tb, err := s.infoForTable(ctx, req.SchemaName, req.Table.Name, req.Configuration)
	if err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	fields := make(map[string]columnInfo)
	for _, column := range tb.columns {
		fields[column.Name] = column
	}

	if s.Debugging() {
		s.LogDebug("Batch processing earliest start files")
	}

	if err := s.batchProcessEarliestStartFiles(ctx, db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.Debugging() {
		s.LogDebug("Batch processing replace files")
	}

	if err := s.batchReplace(ctx, db, fields, req.ReplaceFiles, req.FileParams, req.Keys, req.Table); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.Debugging() {
		s.LogDebug("Batch processing update files")
	}

	if err := s.batchHistoryUpdate(ctx, db, fields, req); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	if s.Debugging() {
		s.LogDebug("Batch processing delete files")
	}

	if err := s.batchReplace(ctx, db, fields, req.DeleteFiles, req.FileParams, req.Keys, req.Table); err != nil {
		return &pb.WriteBatchResponse{
			Response: &pb.WriteBatchResponse_Warning{
				Warning: &pb.Warning{
					Message: err.Error(),
				},
			},
		}, err
	}

	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) batchProcessEarliestStartFiles(ctx context.Context, db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteHistoryBatchRequest) error {
	return s.processCSVRecords(req.EarliestStartFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.Debugging() {
			s.LogDebug("Processing earliest start file", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		if s.Debugging() {
			s.LogDebug("Earliest start record", "values", values)
		}

		cols, _, err := s.getPKColumnsAndValues(values, req.Table)
		if err != nil {
			return fmt.Errorf("unable to get primary key columns and values for record %v: %w", values, err)
		}

		// Let's remove everything all the records with the same primary key(s)
		// whose `_fivetran_start` is GREATER THAN this `_fivetran_start`.

		vars := map[string]interface{}{}
		for k, v := range values {
			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("column %s not found in the table info: %v", k, fields)
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return err
			}

			vars[k] = typedV
		}

		vars["tb"] = req.Table.Name

		var conds []string

		for _, col := range cols {
			conds = append(conds, fmt.Sprintf("%s = $%s", col, col))
		}

		byID := strings.Join(conds, " AND ")

		res, err := surrealdb.Query[any](
			ctx,
			db,
			"DELETE FROM type::table($tb) WHERE "+byID+" AND _fivetran_start > type::datetime($_fivetran_start);",
			vars,
		)
		if err != nil {
			return fmt.Errorf("unable to delete from table %s: %w", req.Table.Name, err)
		}

		if s.Debugging() {
			s.LogDebug("Removed records", "byID", byID, "_fivetran_start_gt", vars["_fivetran_start"], "result", *res)
		}

		return nil
	})
}

func (s *Server) batchHistoryUpdate(ctx context.Context, db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteHistoryBatchRequest) error {
	return s.processCSVRecords(req.UpdateFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.Debugging() {
			s.LogDebug("Processing update file", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		if s.Debugging() {
			s.LogDebug("batchHistoryUpdate record", "values", values)
		}

		var id any
		if v, ok := values["_fivetran_id"]; ok {
			id = v
		} else if v, ok := values["id"]; ok {
			id = v
		} else {
			return fmt.Errorf("id nor _fivetran_id not found in the record: %v", values)
		}

		thing := models.NewRecordID(req.Table.Name, id)

		var unmodifiedFields []string

		vars := map[string]interface{}{}
		for k, v := range values {
			if k == "id" {
				if s.Debugging() {
					s.LogDebug("Skipping id")
				}
				continue
			}

			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("column %s not found in the table info: %v", k, fields)
			}

			if v == req.FileParams.UnmodifiedString {
				unmodifiedFields = append(unmodifiedFields, k)
				continue
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return err
			}

			vars[k] = typedV
		}

		cols, vals, err := s.getPKColumnsAndValues(values, req.Table)
		if err != nil {
			return fmt.Errorf("unable to get primary key columns and values for record %s: %w", thing, err)
		}

		// There could be one or more unmodified fields even though
		// it is the first time for Fivetran and the connector to upsert this record.
		// We try to obtain the previous values from SurrealDB anyway.
		// In case the record is not found, we are sure that the fields noted as unmodified are actually empty.
		if len(unmodifiedFields) > 0 {
			// Get the preivous values to populate the fields with values set to the unmodeified string
			previousFieldsAndValues, err := s.getPreviousValues(ctx, db, unmodifiedFields, req.Table, cols, vals)
			if err != nil {
				return fmt.Errorf("unable to get previous values for record %s: %w", thing, err)
			}

			for k, v := range previousFieldsAndValues {
				vars[k] = v
			}
		}

		err = s.upsertHistoryMode(ctx, db, thing, vars)
		if err != nil {
			return fmt.Errorf("batchHistoryUpdate failed: %w", err)
		}

		return nil
	})
}

func (s *Server) getPreviousValues(ctx context.Context, db *surrealdb.DB, fields []string, table *pb.Table, pkColumns []string, pkValues []any) (map[string]interface{}, error) {
	// Get the previous values for the thing (where the SurrealDB table field that corresponds to the source table's primary key column matches, and its fivetran_active is true)
	var conds []string

	for _, col := range pkColumns {
		conds = append(conds, fmt.Sprintf("%s = $%s", col, col))
	}

	byID := strings.Join(conds, " AND ")

	vars := map[string]interface{}{
		"tb":     table.Name,
		"fields": fields,
	}

	for i, col := range pkColumns {
		vars[col] = pkValues[i]
	}

	req, err := surrealdb.Query[[]map[string]interface{}](
		ctx,
		db,
		fmt.Sprintf(
			"SELECT type::fields($fields) FROM type::table($tb) WHERE %s AND fivetran_active = true;",
			byID,
		),
		vars,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get previous values for %s where %s: %w", table.Name, byID, err)
	}

	if len(*req) == 0 {
		return nil, fmt.Errorf("got empty query response while getting previous values found for %s where %s", table.Name, byID)
	}

	if len((*req)[0].Result) == 0 {
		// We assume the record has not been created yet.
		// The caller should omit the unmodified fields from the vars,
		// so that SurrealDB will create the record without those fields noted unmodified.
		return nil, nil
	}

	return (*req)[0].Result[0], nil
}

func (s *Server) upsertHistoryMode(ctx context.Context, db *surrealdb.DB, thing models.RecordID, vars map[string]interface{}) error {
	if _, found := vars["id"]; found {
		return fmt.Errorf("id is not allowed to be set in the vars")
	}

	res, err := surrealdb.Upsert[any](ctx, db, thing, vars)
	if err != nil {
		return fmt.Errorf("unable to upsert record %s: %w", thing, err)
	}

	if s.Debugging() {
		s.LogDebug("Added history record", "thing", thing, "vars", vars, "result", *res)
	}

	return nil
}
