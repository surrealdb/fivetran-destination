package migrator

import (
	"context"
	"fmt"
	"strings"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// CopyColumn adds a new column and copies data from the source column to the destination column.
// This is used during schema migrations when Fivetran needs to rename a column while preserving data.
//
// According to the Fivetran Partner SDK documentation, this operation should:
// 1. Add the new column (toColumn) with the same data type as the source column (fromColumn)
// 2. Copy data from the source column to the destination column
//
// If this operation returns an unsupported error, Fivetran will fall back to AlterTable RPC,
// but the new column won't have data from the source column.
func (m *Migrator) CopyColumn(ctx context.Context, schema, table, fromColumn, toColumn string) error {
	// 1. Get the field definition of fromColumn from the table schema
	type InfoForTableResult struct {
		Fields map[string]string `cbor:"fields"`
	}
	infoResults, err := surrealdb.Query[InfoForTableResult](ctx, m.db, fmt.Sprintf("INFO FOR TABLE %s", table), nil)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	if infoResults == nil || len(*infoResults) == 0 {
		return fmt.Errorf("no table info returned for %s", table)
	}

	fields := (*infoResults)[0].Result.Fields
	fromFieldDef, exists := fields[fromColumn]
	if !exists {
		return fmt.Errorf("source column %s does not exist in table %s", fromColumn, table)
	}

	// 2. Create new field definition by replacing field name
	// fromFieldDef is like "DEFINE FIELD name ON table TYPE option<string> COMMENT '...'"
	// Replace the field name to create the new column definition
	toFieldDef := strings.Replace(fromFieldDef, "DEFINE FIELD "+fromColumn+" ", "DEFINE FIELD "+toColumn+" ", 1)

	_, err = surrealdb.Query[any](ctx, m.db, toFieldDef, nil)
	if err != nil {
		return fmt.Errorf("failed to create column %s in table %s: %w", toColumn, table, err)
	}

	// 3. Copy data from source to destination column
	updateQuery := fmt.Sprintf("UPDATE %s SET %s = %s", table, toColumn, fromColumn)
	_, err = surrealdb.Query[any](ctx, m.db, updateQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to copy data from %s to %s in table %s: %w", fromColumn, toColumn, table, err)
	}

	m.LogInfo("Copied column",
		"table", table,
		"from_column", fromColumn,
		"to_column", toColumn,
	)

	return nil
}
