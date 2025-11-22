package migrator

import (
	"context"
	"fmt"
	"strings"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// BatchUpdateIDs moves records from table to a temp table with new IDs,
// then moves them back to the original table. This is useful for updating
// record IDs in batch without violating unique constraints.
//
// Parameters:
//   - table: the table to update IDs in
//   - selectedFields: fields to SELECT (e.g., "id, name, value", "*", or "* OMIT foo, bar")
//   - idExpression: SurrealQL expression for new ID (e.g., "record::id(id) + '_v2'")
//   - insertedFields: fields to INSERT (e.g., "name, value" or "foo2 = foo + '_v2', * OMIT foo")
//   - batchSize: number of records per batch
//   - additionalVars: additional query parameters to pass to the query (can be nil)
//
// The final INSERT uses "<idExpression> AS id, <insertedFields>" as the inserted fields.
// This allows renaming fields without repetition, e.g., using "foo2 = foo + '_v2', * OMIT foo"
// to rename foo to foo2 in the new records.
func (m *Migrator) BatchUpdateIDs(ctx context.Context, table, selectedFields, idExpression, insertedFields string, batchSize int, additionalVars map[string]any) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	tempTable := fmt.Sprintf("_temp_%s", table)

	// 1. Create temp table by copying schema from original table using INFO FOR TABLE
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, m.db, fmt.Sprintf("INFO FOR TABLE %s", table), nil)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	// Create temp table
	_, err = surrealdb.Query[any](ctx, m.db, fmt.Sprintf("DEFINE TABLE %s SCHEMAFULL", tempTable), nil)
	if err != nil {
		return fmt.Errorf("failed to create temp table %s: %w", tempTable, err)
	}

	// Copy field definitions, replacing original table name with temp table name
	if infoResults != nil && len(*infoResults) > 0 {
		fields := (*infoResults)[0].Result.Fields
		for _, fieldDef := range fields {
			// fieldDef is like "DEFINE FIELD name ON products TYPE option<string>"
			// Replace table name to target temp table
			tempFieldDef := strings.Replace(fieldDef, " ON "+table+" ", " ON "+tempTable+" ", 1)
			_, err = surrealdb.Query[any](ctx, m.db, tempFieldDef, nil)
			if err != nil {
				m.LogInfo("Warning: could not copy field definition", "error", err.Error())
			}
		}
	}

	// 2. Move records from original to temp with new IDs
	toTempInsertedFields := fmt.Sprintf("%s AS id, %s", idExpression, insertedFields)
	err = m.BatchMoveRecords(ctx, table, tempTable, selectedFields, toTempInsertedFields, batchSize, additionalVars)
	if err != nil {
		return fmt.Errorf("failed to move records to temp table: %w", err)
	}

	// 3. Move records back from temp to original (IDs are already updated)
	err = m.BatchMoveRecords(ctx, tempTable, table, "*", "*", batchSize, nil)
	if err != nil {
		return fmt.Errorf("failed to move records back from temp table: %w", err)
	}

	// 4. Remove temp table
	_, err = surrealdb.Query[any](ctx, m.db, fmt.Sprintf("REMOVE TABLE %s", tempTable), nil)
	if err != nil {
		return fmt.Errorf("failed to remove temp table %s: %w", tempTable, err)
	}

	m.LogInfo("Batch ID update completed",
		"table", table,
		"id_expression", idExpression,
	)

	return nil
}
