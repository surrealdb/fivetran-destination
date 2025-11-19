package migrator

import (
	"context"
)

// DropColumnInHistoryMode removes a column from history-mode tables while maintaining historical records.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#drop_column_in_history_mode
//
// According to the Fivetran Partner SDK documentation, this operation should:
// 1. Validate non-empty table and max(_fivetran_start) < operation_timestamp
// 2. INSERT new rows with NULL for dropped column where it was previously non-null
// 3. UPDATE new rows with operation_timestamp as _fivetran_start
// 4. UPDATE prior active records: _fivetran_end = operation_timestamp - 1, _fivetran_active = FALSE
//
// This follows a similar pattern to ADD_COLUMN_IN_HISTORY_MODE but inserts NULL values.
func (m *Migrator) DropColumnInHistoryMode(ctx context.Context, schema, table, column string, operationTimestamp string) error {
	// TODO: Implement drop column in history mode logic
	// 1. Validate table is non-empty:
	//    SELECT count() FROM table GROUP ALL
	// 2. Validate max(_fivetran_start) < operation_timestamp:
	//    SELECT math::max(_fivetran_start) FROM table GROUP ALL
	// 3. Insert new rows to record the history of the DDL operation.
	//    For each currently active record where column IS NOT NONE:
	//      SELECT * FROM table WHERE _fivetran_active = true AND column != NONE AND _fivetran_start < operation_timestamp
	//    INSERT new row with:
	//       - Same Fivetran primary key values (Note SurrealDB record ID has Fivetran PK values and _fivetran_start hence new row)
	//       - column = NONE in SurrealDB
	//       - _fivetran_start = operation_timestamp
	//       - _fivetran_end = 9999-12-31T23:59:59Z
	//       - _fivetran_active = true
	//  4. Update the previous record's _fivetran_end to (operation timestamp) - 1ms and set _fivetran_active to FALSE:
	//    SELECT * FROM table WHERE _fivetran_active = true AND column != NONE AND _fivetran_start < operation_timestamp
	//    UPDATE original record:
	//       - _fivetran_end = operation_timestamp - 1ms
	//       - _fivetran_active = false
	// 4. Return any errors encountered

	return nil
}
