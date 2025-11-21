package migrator

import (
	"context"
	"fmt"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

// DropColumn removes a column from non-history-mode tables.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#drop_column
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Execute: ALTER TABLE <schema.table> DROP COLUMN <column_name>
func (m *Migrator) DropColumn(ctx context.Context, schema, table, column string) error {
	// 1. Remove the field definition from the table schema
	removeQuery := fmt.Sprintf("REMOVE FIELD %s ON %s", column, table)
	_, err := surrealdb.Query[any](ctx, m.db, removeQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to drop column %s from table %s: %w", column, table, err)
	}

	// 2. Remove the field values from all existing records
	updateQuery := fmt.Sprintf("UPDATE %s UNSET %s", table, column)
	_, err = surrealdb.Query[any](ctx, m.db, updateQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to remove values for column %s in table %s: %w", column, table, err)
	}

	m.LogInfo("Dropped column",
		"table", table,
		"column", column,
	)

	return nil
}
