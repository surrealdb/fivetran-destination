package migrator

import (
	"context"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// AddColumnInHistoryMode adds a column to history-mode tables while preserving historical record integrity.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#add_column_in_history_mode
//
// According to the Fivetran Partner SDK documentation, this operation should:
// 1. Validate table is non-empty and max(_fivetran_start) < operation_timestamp
// 2. Execute: ALTER TABLE <schema.table> ADD COLUMN <column_name> <column_type>
// 3. INSERT new history rows with default value and operation_timestamp as _fivetran_start
// 4. UPDATE newly created rows to set column value
// 5. UPDATE previous active records: set _fivetran_end = operation_timestamp - 1ms, _fivetran_active = FALSE
//
// This maintains temporal integrity through timestamp-based state transitions.
func (m *Migrator) AddColumnInHistoryMode(ctx context.Context, schema, table, column string, columnType pb.DataType, defaultValue string, operationTimestamp string) error {
	// TODO: Implement add column in history mode logic
	// 1. Validate table is non-empty:
	//    SELECT count() FROM table GROUP ALL
	// 2. Validate max(_fivetran_start) < operation_timestamp:
	//    SELECT math::max(_fivetran_start) FROM table GROUP ALL
	// 3. Add the new field:
	//    DEFINE FIELD OVERWRITE column ON table TYPE <mapped_type>
	// 4. For each currently active record (_fivetran_active = true):
	//    a. INSERT new row with:
	//       - Same primary key values
	//       - column = default_value
	//       - _fivetran_start = operation_timestamp
	//       - _fivetran_end = 9999-12-31T23:59:59Z
	//       - _fivetran_active = true
	//    b. UPDATE original record:
	//       - _fivetran_end = operation_timestamp - 1ms
	//       - _fivetran_active = false
	// 5. Return any errors encountered

	return nil
}
