package migrator

import (
	"context"
	"fmt"

	"github.com/surrealdb/fivetran-destination/internal/connector/tablemapper"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

// RemoveSurrealDBFieldsNotInFivetranTable removes fields from a SurrealDB table that are not present in the Fivetran table schema.
func (m *Migrator) RemoveSurrealDBFieldsNotInFivetranTable(ctx context.Context, db *surrealdb.DB, schemaName string, table *pb.Table) error {
	desiredColumns := make(map[string]bool)
	for _, c := range table.Columns {
		desiredColumns[c.Name] = true
	}

	tm := tablemapper.New(db, m.Logging)

	currentTableInfo, err := tm.InfoForTable(ctx, table.Name)
	if err != nil {
		return fmt.Errorf("failed to get current table info for table %s: %v", table.Name, err)
	}

	for _, c := range currentTableInfo.Columns {
		if _, ok := desiredColumns[c.Name]; !ok {
			if err := m.DropColumn(ctx, schemaName, table.Name, c.Name); err != nil {
				return fmt.Errorf("failed to remove field %s from table %s: %v", c.Name, table.Name, err)
			}
			tm.LogInfo("Removed SurrealDB field not in Fivetran table", "table", table.Name, "field", c.Name)
		}
	}

	return nil
}
