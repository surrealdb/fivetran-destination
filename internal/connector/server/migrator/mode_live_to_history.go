package migrator

import (
	"context"
	"fmt"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// ModeLiveToHistory converts a live-mode table to history mode for temporal tracking.
//
// Reference: https://github.com/fivetran/fivetran_partner_sdk/blob/main/schema-migration-helper-service.md#live_to_history
//
// According to the Fivetran Partner SDK documentation, this operation should:
//   - Add history columns (_fivetran_start, _fivetran_end, _fivetran_active)
//     and populate with appropriate timestamps for all existing records
//
// This enables transition from simple current-state tracking to full
// historical record maintenance.
func (m *Migrator) ModeLiveToHistory(ctx context.Context, schema, table string) error {
	const batchSize = 1000
	now := models.CustomDateTime{Time: time.Now().UTC()}
	endTimeMax := models.CustomDateTime{Time: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)}

	// 1. Add history mode field definitions
	historyFields := []string{
		fmt.Sprintf("DEFINE FIELD _fivetran_start ON %s TYPE option<datetime>", table),
		fmt.Sprintf("DEFINE FIELD _fivetran_end ON %s TYPE option<datetime>", table),
		fmt.Sprintf("DEFINE FIELD _fivetran_active ON %s TYPE option<bool>", table),
	}
	for _, fieldDef := range historyFields {
		_, err := surrealdb.Query[any](ctx, m.db, fieldDef, nil)
		if err != nil {
			return fmt.Errorf("failed to add history field: %w", err)
		}
	}

	// 2. Initialize all existing records with history values
	updateQuery := fmt.Sprintf(`UPDATE %s SET
		_fivetran_start = $now,
		_fivetran_end = $end_max,
		_fivetran_active = true`, table)
	_, err := surrealdb.Query[any](ctx, m.db, updateQuery, map[string]any{
		"now":     now,
		"end_max": endTimeMax,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize history fields: %w", err)
	}

	// 3. Update IDs to include _fivetran_start component
	// ID transformation: [pk1, pk2, ...] -> [pk1, pk2, ..., _fivetran_start]
	idExpression := "array::add(record::id(id), _fivetran_start)"
	insertedFields := "*"

	err = m.BatchUpdateIDs(ctx, table, "*", idExpression, insertedFields, batchSize)
	if err != nil {
		return fmt.Errorf("failed to update record IDs: %w", err)
	}

	m.LogInfo("Converted table from live to history mode",
		"table", table,
	)

	return nil
}
