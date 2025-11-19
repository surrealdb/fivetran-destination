package migrator

import (
	"context"
)

// DropColumn removes a column from non-history-mode tables.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#drop_column
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Execute: ALTER TABLE <schema.table> DROP COLUMN <column_name>
func (m *Migrator) DropColumn(ctx context.Context, schema, table, column string) error {
	// TODO: Implement drop column logic
	// 1. Remove the field from the table:
	//    REMOVE FIELD column ON table
	// 2. Return any errors encountered

	return nil
}
