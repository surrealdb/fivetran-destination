package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// writeHistoryBatch handles the WriteHistoryBatch request from Fivetran.
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

	db, err := s.connectAndUse(ctx, cfg, req.SchemaName)
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

	// Implements https://github.com/fivetran/fivetran_partner_sdk/blob/main/how-to-handle-history-mode-batch-files.md#earliest_start_files
	//
	// See "EARLIEST START FILE" in https://github.com/fivetran/fivetran_partner_sdk/blob/main/history_mode.png
	if err := s.handleHistoryModeEarliestStartFiles(ctx, db, fields, req); err != nil {
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

	// Implements https://github.com/fivetran/fivetran_partner_sdk/blob/main/how-to-handle-history-mode-batch-files.md#replace_files
	//
	// We assume this corresponds to "UPSERT BATCH FILE" in https://github.com/fivetran/fivetran_partner_sdk/blob/main/history_mode.png
	if err := s.handleHistoryModeReplaceFiles(ctx, db, fields, req.ReplaceFiles, req.FileParams, req.Keys, req.Table); err != nil {
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

	// Implements https://github.com/fivetran/fivetran_partner_sdk/blob/main/how-to-handle-history-mode-batch-files.md#update_files
	//
	// We assume this corresponds to "UPDATE BATCH FILE" in https://github.com/fivetran/fivetran_partner_sdk/blob/main/history_mode.png
	if err := s.handleHistoryModeUpdateFiles(ctx, db, fields, req); err != nil {
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

	// Implements https://github.com/fivetran/fivetran_partner_sdk/blob/main/how-to-handle-history-mode-batch-files.md#delete_files
	//
	// TODO We probably need to have handleDeleteFiles specifically for DeleteFiles
	// Once that's done this will correspond to "DELETE BATCH FILE" in
	// https://github.com/fivetran/fivetran_partner_sdk/blob/main/history_mode.png
	if err := s.handleHistoryModeDeleteFiles(ctx, db, fields, req); err != nil {
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

func (s *Server) handleHistoryModeEarliestStartFiles(ctx context.Context, db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteHistoryBatchRequest) error {
	return s.processCSVRecords(req.EarliestStartFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.Debugging() {
			s.LogDebug("Processing earliest start file", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		if s.Debugging() {
			s.LogDebug("Earliest start record", "commaSeparatedStringValues", values)
		}

		cols, pkVals, err := s.getPKColumnsAndValues(values, req.Table, fields)
		if err != nil {
			return fmt.Errorf("unable to get primary key columns and values for record %v: %w", values, err)
		}

		// Let's remove everything all the records with the same primary key(s)
		// whose `_fivetran_start` is GREATER THAN this `_fivetran_start`.

		vars := map[string]interface{}{}
		for k, v := range values {
			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("history mode earliest start file: column %s not found in the table info: %v", k, fields)
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return fmt.Errorf("earliest start file: %w", err)
			}

			vars[k] = typedV
		}

		vars["tb"] = req.Table.Name

		var pkCondsExceptFtStart []string

		for _, col := range cols {
			if col == "_fivetran_start" {
				// We don't want to include _fivetran_start in the equality conditions
				// because we want to delete records whose _fivetran_start is greater than or equal to the given one.
				continue
			}
			pkCondsExceptFtStart = append(pkCondsExceptFtStart, fmt.Sprintf("%s = $%s", col, col))
		}

		byPksExceptFtStart := strings.Join(pkCondsExceptFtStart, " AND ")
		res, err := surrealdb.Query[any](
			ctx,
			db,
			"DELETE FROM type::table($tb) WHERE "+byPksExceptFtStart+" AND _fivetran_start >= type::datetime($_fivetran_start);",
			vars,
		)
		if err != nil {
			return fmt.Errorf("unable to delete from table %s: %w", req.Table.Name, err)
		}

		if s.Debugging() {
			s.LogDebug("Removed records", "byID", byPksExceptFtStart, "_fivetran_start_gt", vars["_fivetran_start"], "result", *res)
		}

		res, err = surrealdb.Query[any](
			ctx,
			db,
			"SELECT _fivetran_start FROM type::table($tb) WHERE "+byPksExceptFtStart+" ORDER BY _fivetran_start DESC LIMIT 1;",
			vars,
		)
		if err != nil {
			return fmt.Errorf("unable to select from table %s: %w", req.Table.Name, err)
		}

		if s.Debugging() {
			s.LogDebug("Selected latest _fivetran_start", "byID", byPksExceptFtStart, "result", *res)
		}

		if len(*res) == 0 {
			// No existing records remain, nothing to do.
			if s.Debugging() {
				s.LogDebug("No existing records remain after earliest_start removal, skipping update to set _fivetran_active and _fivetran_end", "byID", byPksExceptFtStart)
			}
			return nil
		}

		latestFtStartRecords := (*res)[0].Result.([]any)
		switch len(latestFtStartRecords) {
		case 0:
			// No existing records remain, nothing to do.
			if s.Debugging() {
				s.LogDebug("No existing records remain after earliest_start removal, skipping update to set _fivetran_active and _fivetran_end", "byID", byPksExceptFtStart)
			}
			return nil
		case 1:
			// OK
		default:
			return fmt.Errorf("expected 0 or 1 latest _fivetran_start record, got %d", len(latestFtStartRecords))
		}
		latestFtStartRecord, ok := latestFtStartRecords[0].(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected type for latest _fivetran_start record: %T", latestFtStartRecords[0])
		}
		latestFtStartVal, ok := latestFtStartRecord["_fivetran_start"]
		if !ok {
			return fmt.Errorf("_fivetran_start not found in the selected record: %v", latestFtStartRecord)
		}

		latestFtStart, ok := latestFtStartVal.(models.CustomDateTime)
		if !ok {
			return fmt.Errorf("unexpected type for _fivetran_start: %T", latestFtStartVal)
		}

		pkVals[len(pkVals)-1] = latestFtStart

		updatedRecordID := models.NewRecordID(req.Table.Name, pkVals)

		earliestStart, ok := vars["_fivetran_start"].(models.CustomDateTime)
		if !ok {
			return fmt.Errorf("unexpected type for _fivetran_start in vars: %T", vars["_fivetran_start"])
		}

		endTime := models.CustomDateTime{
			Time: earliestStart.Add(-time.Millisecond),
		}

		res, err = surrealdb.Query[any](
			ctx,
			db,
			"UPDATE type::thing($record_id) SET _fivetran_active = false, _fivetran_end = $_fivetran_end",
			map[string]any{
				"record_id":     updatedRecordID,
				"_fivetran_end": endTime,
			},
		)
		if err != nil {
			return fmt.Errorf("unable to update record %v: %w", updatedRecordID, err)
		}

		if s.Debugging() {
			s.LogDebug("Updated records to set _fivetran_active=false and _fivetran_end=_fivetran_start-1ms", "_fivetran_start", vars["_fivetran_start"], "result", *res)
		}

		return nil
	})
}

func (s *Server) getPKColumnsAndValuesTyped(values map[string]any, table *pb.Table) ([]string, []any, error) {
	var pkColumns []string
	for _, c := range table.Columns {
		if c.PrimaryKey {
			pkColumns = append(pkColumns, c.Name)
		}
	}

	s.LogDebug("getPKColumnsAndValuesTyped: primary key columns", "table", table.Name, "pkColumns", pkColumns)

	// Note that we intentionally do not sort the primary key columns.
	// We assume Fivetran in history mode sends us primary key columns containing the primary key in the source
	// along with _fivetran_start.
	// In that case, I want to use [id_from_src, _fivetran_start] as the primary key assumng
	// Fivetran gives us columns definitions in this specific order.
	// If we sorted it like the below, we might end up with [_fivetran_start, id_from_src] as the primary key.
	// That's not wrong but I think it's not intuitive from users perspective.
	//
	// sort.Slice(pkColumns, func(i, j int) bool {
	// 	return pkColumns[i] < pkColumns[j]
	// })

	var pkValues []any
	for _, pkColumn := range pkColumns {
		v, ok := values[pkColumn]
		if !ok {
			return nil, nil, fmt.Errorf("primary key column %s not found in record values: %v", pkColumn, values)
		}
		pkValues = append(pkValues, v)
	}

	return pkColumns, pkValues, nil
}

func (s *Server) generateIdArray(values map[string]string, table *pb.Table, fields map[string]columnInfo) ([]any, error) {
	_, vals, err := s.getPKColumnsAndValues(values, table, fields)
	if err != nil {
		return nil, fmt.Errorf("unable to get primary key columns and values for record %v: %w", values, err)
	}

	return vals, nil
}

func (s *Server) generateIdArrayForDelete(values map[string]string, table *pb.Table, fields map[string]columnInfo, fivetranStartDefault *models.CustomDateTime) ([]any, error) {
	_, vals, err := s.getPKColumnsAndValuesForDelete(values, table, fields)
	if err != nil {
		return nil, fmt.Errorf("generateIdArrayForDelete: unable to get primary key columns and values for record %v: %w", values, err)
	}

	// Override _fivetran_start value to make the composite key
	vals = append(vals, fivetranStartDefault)

	return vals, nil
}

func (s *Server) generateIdArrayTyped(values map[string]any, table *pb.Table) (*models.RecordID, error) {
	_, vals, err := s.getPKColumnsAndValuesTyped(values, table)
	if err != nil {
		return nil, fmt.Errorf("generateIdArrayTyped: unable to get primary key columns and values for record %v: %w", values, err)
	}

	rid := models.NewRecordID(table.Name, vals)

	return &rid, nil
}

// Reads CSV files and replaces existing records accordingly.
func (s *Server) handleHistoryModeReplaceFiles(ctx context.Context, db *surrealdb.DB, fields map[string]columnInfo, replaceFiles []string, fileParams *pb.FileParams, keys map[string][]byte, table *pb.Table) error {
	unmodifiedString := fileParams.UnmodifiedString
	return s.processCSVRecords(replaceFiles, fileParams, keys, func(columns []string, record []string) error {
		if s.Debugging() {
			s.LogDebug("Replacing record", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		idArr, err := s.generateIdArray(values, table, fields)
		if err != nil {
			return fmt.Errorf("history mode replace file: %w", err)
		}

		thing := models.NewRecordID(table.Name, idArr)

		vars := map[string]any{}
		for k, v := range values {
			if unmodifiedString != "" && v == unmodifiedString {
				if s.Debugging() {
					s.LogDebug("Skipping unmodified column", "column", k, "value", v)
				}
				continue
			}

			if k == "id" {
				if s.Debugging() {
					s.LogDebug("Skipping id column")
				}
				continue
			}

			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("replace file: column %s not found in the table info: %v", k, fields)
			}

			if v == fileParams.NullString {
				vars[k] = models.None
				continue
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return fmt.Errorf("replace file: %w", err)
			}

			vars[k] = typedV
		}

		res, err := surrealdb.Upsert[any](ctx, db, thing, vars)
		if err != nil {
			if s.metrics != nil {
				s.metrics.DBWriteError()
			}
			s.LogDebug("Failed to upsert record for replace", "thing", thing, "vars", fmt.Sprintf("%+v", vars), "error", err)
			return fmt.Errorf("unable to upsert record %s: %w", thing, err)
		}

		// Track successful DB write
		if s.metrics != nil {
			s.metrics.DBWriteCompleted(1)
		}

		if s.Debugging() {
			s.LogDebug("Replaced record", "commaSeparatedStringValues", values, "thing", thing, "vars", fmt.Sprintf("%+v", vars), "result", fmt.Sprintf("%+v", *res))
		}

		return nil
	})
}

func (s *Server) getLatestFivetranStartInStr(ctx context.Context, db *surrealdb.DB, table *pb.Table, values map[string]string, fields map[string]columnInfo) (*models.CustomDateTime, error) {
	var conds []string
	vars := map[string]interface{}{
		"tb": table.Name,
	}

	cols, vals, err := s.getPKColumnsAndValuesForDelete(values, table, fields)
	if err != nil {
		return nil, fmt.Errorf("latest fivetran_start: %w", err)
	}

	for i, col := range cols {
		if col == "_fivetran_start" {
			// We don't want to include _fivetran_start in the WHERE clause
			// to get the latest _fivetran_start
			continue
		}
		vars[col] = vals[i]
		conds = append(conds, fmt.Sprintf("%s = $%s", col, col))
	}

	byID := strings.Join(conds, " AND ")

	req, err := surrealdb.Query[[]map[string]interface{}](
		ctx,
		db,
		fmt.Sprintf(
			"SELECT _fivetran_start FROM type::table($tb) WHERE %s ORDER BY _fivetran_start DESC LIMIT 1;",
			byID,
		),
		vars,
	)

	if err != nil {
		return nil, fmt.Errorf("unable to get latest _fivetran_start for %s where %s: %w", table.Name, byID, err)
	}

	if len(*req) == 0 {
		return nil, fmt.Errorf("got empty query response while getting latest _fivetran_start for %s where %s", table.Name, byID)
	}

	if len((*req)[0].Result) == 0 {
		return nil, fmt.Errorf("got empty result while getting latest _fivetran_start for %s where %s", table.Name, byID)
	}

	ftStart := (*req)[0].Result[0]["_fivetran_start"]

	latestFivetranStart, ok := ftStart.(models.CustomDateTime)
	if !ok {
		return nil, fmt.Errorf("unable to assert latest _fivetran_start to SurrealDB datetime for %s where %s: %+v", table.Name, byID, ftStart)
	}

	return &latestFivetranStart, nil
}

func (s *Server) getPKColumnsAndValuesForDelete(strValues map[string]string, table *pb.Table, fields map[string]columnInfo) ([]string, []any, error) {
	var pkColumns []string
	for _, c := range table.Columns {
		if c.PrimaryKey && c.Name != "_fivetran_start" {
			pkColumns = append(pkColumns, c.Name)
		}
	}

	// Note that we intentionally do not sort the primary key columns.
	// We assume Fivetran in history mode sends us primary key columns containing the primary key in the source
	// along with _fivetran_start.
	// In that case, I want to use [id_from_src, _fivetran_start] as the primary key assumng
	// Fivetran gives us columns definitions in this specific order.
	// If we sorted it like the below, we might end up with [_fivetran_start, id_from_src] as the primary key.
	// That's not wrong but I think it's not intuitive from users perspective.
	//
	// sort.Slice(pkColumns, func(i, j int) bool {
	// 	return pkColumns[i] < pkColumns[j]
	// })

	var pkValues []any
	for _, pkColumn := range pkColumns {
		v, ok := strValues[pkColumn]
		if !ok {
			return nil, nil, fmt.Errorf("primary key column %s not found in record values: %v", pkColumn, strValues)
		}

		f, ok := fields[pkColumn]
		if !ok {
			return nil, nil, fmt.Errorf("getPKColumnsAndValues: column %s not found in the table info: %v", pkColumn, fields)
		}

		typedV, err := f.strToSurrealType(v)
		if err != nil {
			return nil, nil, fmt.Errorf("getPKColumnsAndValues: %w", err)
		}

		pkValues = append(pkValues, typedV)
	}

	return pkColumns, pkValues, nil
}

func (s *Server) handleHistoryModeUpdateFiles(ctx context.Context, db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteHistoryBatchRequest) error {
	return s.processCSVRecords(req.UpdateFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.Debugging() {
			s.LogDebug("Processing update file", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		if s.Debugging() {
			s.LogDebug("batchHistoryUpdate record", "commaSeparatedStringValues", values)
		}

		id, err := s.generateIdArray(values, req.Table, fields)
		if err != nil {
			return fmt.Errorf("history mode update file: %w", err)
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
				return fmt.Errorf("history mode update file: column %s not found in the table info: %v", k, fields)
			}

			if v == req.FileParams.UnmodifiedString {
				unmodifiedFields = append(unmodifiedFields, k)
				continue
			}

			// Null strings like "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP"
			// should result in SurrealDB none for the option<theType> SurrealDB
			// field.
			if v == req.FileParams.NullString {
				vars[k] = models.None
				continue
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return fmt.Errorf("history mode update file: %w", err)
			}

			vars[k] = typedV
		}

		cols, vals, err := s.getPKColumnsAndValues(values, req.Table, fields)
		if err != nil {
			return fmt.Errorf("unable to get primary key columns and values for record %s: %w", thing, err)
		}

		if len(unmodifiedFields) == 0 {
			// We assume it is invalid to have no unmodified fields in an update file.
			return fmt.Errorf("history mode update file: no unmodified fields found in the record %s", thing)
		}

		// Get the preivous values to populate the fields with values set to the unmodeified string

		// There could be one or more unmodified fields even though
		// it is the first time for Fivetran and the connector to upsert this record.
		// We try to obtain the previous values from SurrealDB anyway.
		// In case the record is not found, we are sure that the fields noted as unmodified are actually empty.
		previousPKValues, previousFieldsAndValues, err := s.getPreviousValues(ctx, db, unmodifiedFields, req.Table, cols, vals)
		if err != nil {
			return fmt.Errorf("unable to get previous values for record %s: %w", thing, err)
		}

		for k, v := range previousFieldsAndValues {
			vars[k] = v
		}

		prevThing, err := s.generateIdArrayTyped(previousPKValues, req.Table)
		if err != nil {
			return fmt.Errorf("history mode update file while generating previous thing: %w", err)
		}

		newStartTime, ok := vars["_fivetran_start"].(models.CustomDateTime)
		if !ok {
			return fmt.Errorf("unable to assert _fivetran_start to models.CustomDateTime for record %s: %+v", thing, vars["_fivetran_start"])
		}

		prevEndTime := newStartTime.Add(-1 * time.Millisecond)

		// Update the previous record to set its _fivetran_active to false,
		// and _fivetran_end to newStartTime-1ms
		err = s.upsertSetHistoryMode(ctx, db, *prevThing, map[string]interface{}{
			"_fivetran_active": false,
			"_fivetran_end":    prevEndTime,
		})
		if err != nil {
			return fmt.Errorf("batchHistoryUpdate failed to update previous record's _fivetran_end: %w", err)
		}

		err = s.upsertContentHistoryMode(ctx, db, thing, vars)
		if err != nil {
			return fmt.Errorf("batchHistoryUpdate failed: %w", err)
		}

		return nil
	})
}

func (s *Server) getPreviousValues(ctx context.Context, db *surrealdb.DB, fields []string, table *pb.Table, pkColumns []string, pkValues []any) (map[string]any, map[string]any, error) {
	// Get the previous values for the thing (where the SurrealDB table field that corresponds to the source table's primary key column matches, and its fivetran_active is true)
	var conds []string

	for _, col := range pkColumns {
		if col == "_fivetran_start" {
			// As we want the latest active record, we do not include _fivetran_start in the WHERE clause.
			continue
		}
		conds = append(conds, fmt.Sprintf("%s = $%s", col, col))
	}

	byID := strings.Join(conds, " AND ")

	idFieldsAndContentFields := make([]string, len(pkColumns)+len(fields))
	copy(idFieldsAndContentFields, pkColumns)
	copy(idFieldsAndContentFields[len(pkColumns):], fields)

	vars := map[string]interface{}{
		"tb":     table.Name,
		"fields": idFieldsAndContentFields,
	}

	for i, col := range pkColumns {
		vars[col] = pkValues[i]
	}

	query := fmt.Sprintf(
		"SELECT type::fields($fields) FROM type::table($tb) WHERE %s AND _fivetran_active = true;",
		byID,
	)

	req, err := surrealdb.Query[[]map[string]interface{}](
		ctx,
		db,
		query,
		vars,
	)
	resDebug, errDebug := surrealdb.Query[[]map[string]any](
		ctx,
		db,
		"SELECT * FROM type::table($tb)",
		map[string]any{
			"tb": table.Name,
		},
	)
	s.LogDebug(
		"Executed SurrealQL query",
		"func", "getPreviousValues",
		"query", query,
		"vars", vars,
		"err", err,
		"response", req,
		"debugRes", resDebug,
		"debugErr", errDebug,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get previous values for %s where %s: %w", table.Name, byID, err)
	}

	if len(*req) == 0 {
		return nil, nil, fmt.Errorf("got empty query response while getting previous values found for %s where %s", table.Name, byID)
	}

	if len((*req)[0].Result) == 0 {
		// We assume the record has not been created yet.
		// The caller should omit the unmodified fields from the vars,
		// so that SurrealDB will create the record without those fields noted unmodified.
		return nil, nil, nil
	}

	if len((*req)[0].Result) > 1 {
		return nil, nil, fmt.Errorf("got multiple results while getting previous values found for %s where %s: %+v", table.Name, byID, (*req)[0].Result)
	}

	fetchedPKValues := make(map[string]any)
	fetchedContentValues := make(map[string]any)
	for k, v := range (*req)[0].Result[0] {
		isPK := false
		for _, pkCol := range pkColumns {
			if k == pkCol {
				isPK = true
				break
			}
		}
		if isPK {
			fetchedPKValues[k] = v
		} else {
			fetchedContentValues[k] = v
		}
	}

	return fetchedPKValues, fetchedContentValues, nil
}

func (s *Server) upsertSetHistoryMode(ctx context.Context, db *surrealdb.DB, thing models.RecordID, vars map[string]interface{}) error {
	var conds []string
	for k := range vars {
		if k == "thing" {
			continue
		}
		conds = append(conds, fmt.Sprintf("%s = $%s", k, k))
	}

	vars["thing"] = thing

	_, err := surrealdb.Query[[]map[string]any](
		ctx,
		db,
		"UPSERT $thing SET "+strings.Join(conds, ", "),
		vars,
	)
	if err != nil {
		return fmt.Errorf("upsert set failed: %w", err)
	}

	return nil
}

func (s *Server) upsertContentHistoryMode(ctx context.Context, db *surrealdb.DB, thing models.RecordID, vars map[string]interface{}) error {
	if _, found := vars["id"]; found {
		return fmt.Errorf("id is not allowed to be set in the vars")
	}

	res, err := surrealdb.Upsert[any](ctx, db, thing, vars)
	if err != nil {
		s.LogDebug("Failed to upsert record for update", "thing", thing, "vars", fmt.Sprintf("%+v", vars), "error", err)
		return fmt.Errorf("unable to upsert record %s: %w", thing, err)
	}

	if s.Debugging() {
		s.LogDebug("Added history record", "thing", thing, "vars", vars, "result", *res)
	}

	return nil
}

func (s *Server) handleHistoryModeDeleteFiles(ctx context.Context, db *surrealdb.DB, fields map[string]columnInfo, req *pb.WriteHistoryBatchRequest) error {
	return s.processCSVRecords(req.DeleteFiles, req.FileParams, req.Keys, func(columns []string, record []string) error {
		if s.Debugging() {
			s.LogDebug("Processing delete file", "columns", columns, "record", record)
		}

		values := make(map[string]string)
		for i, column := range columns {
			values[column] = record[i]
		}

		// In case it is DELETE file, Fivetran does not provide _fivetran_start column/value.
		// In that case, we need to be creative to get the lastest _fivetran_start for the record
		// identified by the primary key columns.
		// That way, we can update the (1) fivetran_end to the time specified in the file,
		// and (2) fivetran_active to false for the latest record.
		latestFivetranStart, err := s.getLatestFivetranStartInStr(ctx, db, req.Table, values, fields)
		if err != nil {
			return fmt.Errorf("unable to get latest _fivetran_start for record %v: %w", values, err)
		}

		if s.Debugging() {
			s.LogDebug("History mode delete record", "commaSeparatedStringValues", values)
		}

		id, err := s.generateIdArrayForDelete(values, req.Table, fields, latestFivetranStart)
		if err != nil {
			return fmt.Errorf("history mode delete file: %w", err)
		}

		vars := map[string]any{
			"thing": models.NewRecordID(req.Table.Name, id),
		}
		for k, v := range values {
			if k == "id" {
				if s.Debugging() {
					s.LogDebug("Skipping id")
				}
				continue
			}

			f, ok := fields[k]
			if !ok {
				return fmt.Errorf("history mode delete file: column %s not found in the table info: %v", k, fields)
			}

			if v == req.FileParams.UnmodifiedString {
				continue
			}

			// Null strings like "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP"
			// should be handled as "missing" and "not neeeded to be updated"
			// in DELETE files.
			if v == req.FileParams.NullString {
				continue
			}

			var typedV interface{}

			typedV, err := f.strToSurrealType(v)
			if err != nil {
				return fmt.Errorf("history mode delete file: %w", err)
			}

			vars[k] = typedV
		}

		var conds []string
		for k := range vars {
			if k == "thing" {
				continue
			}
			conds = append(conds, fmt.Sprintf("%s = $%s", k, k))
		}

		_, err = surrealdb.Query[[]map[string]any](
			ctx,
			db,
			"UPSERT $thing SET "+strings.Join(conds, ", "),
			vars,
		)
		if err != nil {
			return fmt.Errorf("history mode delete file failed: %w", err)
		}

		return nil
	})
}
