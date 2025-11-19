package migrator

import (
	"context"
)

// UpdateColumnValue updates all values in a specified column with a new value.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#update_column_value_operation
//
// According to the Fivetran Partner SDK documentation, this operation should:
// - Execute: UPDATE <schema.table> SET <column_name> = <new_value>
//
// NULL is supported as a valid update value.
func (m *Migrator) UpdateColumnValue(ctx context.Context, schema, table, column, value string) error {
	// TODO: Implement update column value logic
	// 1. Update all rows in the table with the new value:
	//    UPDATE table SET column = value
	// 2. Handle NULL value case:
	//    If value represents NULL, use: UPDATE table SET column = NONE
	// 3. Return any errors encountered

	return nil
}
