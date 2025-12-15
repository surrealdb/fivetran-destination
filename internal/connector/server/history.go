package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// rangeQueryConfig holds the configuration for building a range query on Record IDs.
// This is used when "id" is a primary key column.
type rangeQueryConfig struct {
	lowerBound []any  // PK values excluding _fivetran_start
	upperBound []any  // PK values + max datetime
	byID       string // Description for error messages
}

// buildRecordIDRangeQueryBounds builds the lower and upper bounds for a range query on Record IDs.
// This is used when "id" is a primary key column in history mode tables.
//
// The query uses:
//
//	WHERE id >= type::thing($tb, $lower) AND id < type::thing($tb, $upper)
//
// Where $lower is [pk_values...] and $upper is [pk_values..., max_datetime]
func buildRecordIDRangeQueryBounds(pkColumns []string, pkValues []any) *rangeQueryConfig {
	// Build lower bound: PK values excluding _fivetran_start
	var lowerBound []any
	for i, col := range pkColumns {
		if col == "_fivetran_start" {
			continue
		}
		lowerBound = append(lowerBound, pkValues[i])
	}

	// Build upper bound: PK values + max datetime
	maxTime := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	upperBound := make([]any, len(lowerBound)+1)
	copy(upperBound, lowerBound)
	upperBound[len(lowerBound)] = models.CustomDateTime{Time: maxTime}

	return &rangeQueryConfig{
		lowerBound: lowerBound,
		upperBound: upperBound,
		byID:       "id >= type::thing($tb, $lower) AND id < type::thing($tb, $upper)",
	}
}

// buildRangeQuerySubquery builds the subquery for range query on Record IDs.
// selectFields is the SELECT clause content (e.g., "_fivetran_start, id" or "type::fields($fields), id")
//
// We need a subquery approach for the following reasons:
//
//  1. ORDER BY id DESC fails with "The underlying datastore does not support reversed scans"
//     when used directly with a WHERE clause on Record IDs. This error may not occur with
//     SurrealDB datastores that support reverse scans, but we use this universal approach
//     for broader compatibility.
//
//  2. ORDER BY id.id DESC works but returns id.id as [nil, nil] when used with type::fields,
//     making it impossible to extract the actual Record ID values needed.
//
//  3. Using id.id in WHERE clauses may result in full table scan behavior because it may not
//     be translated directly to the underlying key-value store's key, so it should be avoided
//     if possible.
//
// The subquery workaround computes the comparison in SELECT, filters on the result,
// and preserves the full Record ID for extraction.
func buildRangeQuerySubquery(selectFields string) string {
	return fmt.Sprintf(`SELECT %s FROM (
		SELECT *,
			id >= type::thing($tb, $lower) AS _gte,
			id < type::thing($tb, $upper) AS _lt
		FROM type::table($tb)
	) WHERE _gte = true AND _lt = true ORDER BY id DESC LIMIT 1;`, selectFields)
}

// hasIdPKColumn checks if "id" is one of the primary key columns.
func hasIdPKColumn(pkColumns []string) bool {
	for _, col := range pkColumns {
		if col == "id" {
			return true
		}
	}
	return false
}

// selectLatestHistoryRecord queries for the latest history record matching the given PK values.
// It handles both the "id" column case (using range query) and the standard case (using equality).
//
// Parameters:
//   - selectFields: fields to select (e.g., "_fivetran_start, id" or "type::fields($fields), id")
//   - extraVars: additional variables to include in the query (e.g., "fields" for type::fields)
//   - pkColumns: primary key column names
//   - pkValues: primary key values corresponding to pkColumns
//   - tableName: the SurrealDB table name
//
// Returns the query result or nil if no record found.
func (s *Server) selectLatestHistoryRecord(
	ctx context.Context,
	db *surrealdb.DB,
	selectFields string,
	extraVars map[string]any,
	pkColumns []string,
	pkValues []any,
	tableName string,
) ([]map[string]any, string, error) {
	vars := map[string]any{
		"tb": tableName,
	}
	for k, v := range extraVars {
		vars[k] = v
	}

	var query string
	var byID string

	if hasIdPKColumn(pkColumns) {
		// Range query approach for "id" column
		rangeConfig := buildRecordIDRangeQueryBounds(pkColumns, pkValues)
		vars["lower"] = rangeConfig.lowerBound
		vars["upper"] = rangeConfig.upperBound
		byID = rangeConfig.byID

		if s.Debugging() {
			var lowerTypes, upperTypes []string
			for _, v := range rangeConfig.lowerBound {
				lowerTypes = append(lowerTypes, fmt.Sprintf("%T", v))
			}
			for _, v := range rangeConfig.upperBound {
				upperTypes = append(upperTypes, fmt.Sprintf("%T", v))
			}
			s.LogDebug("selectLatestHistoryRecord range query bounds",
				"lower", rangeConfig.lowerBound,
				"lowerTypes", lowerTypes,
				"upper", rangeConfig.upperBound,
				"upperTypes", upperTypes)
		}

		query = buildRangeQuerySubquery(selectFields)
	} else {
		// Standard equality approach
		var conds []string
		for i, col := range pkColumns {
			if col == "_fivetran_start" {
				continue
			}
			vars[col] = pkValues[i]
			conds = append(conds, fmt.Sprintf("%s = $%s", col, col))
		}
		byID = strings.Join(conds, " AND ")

		// Note: We include _fivetran_start in the selected fields explicitly
		// because that's needed to use _fivetran_start in order-by clause.
		// SurrealDB cannot find that _fivetran_start is included in $fields at query parsing time.
		query = fmt.Sprintf(
			"SELECT %s, _fivetran_start FROM type::table($tb) WHERE %s ORDER BY _fivetran_start DESC LIMIT 1;",
			selectFields,
			byID,
		)
	}

	req, err := surrealdb.Query[[]map[string]any](ctx, db, query, vars)
	if err != nil {
		return nil, byID, fmt.Errorf("query failed: %w", err)
	}

	if len(*req) == 0 {
		return nil, byID, fmt.Errorf("got empty query response")
	}

	return (*req)[0].Result, byID, nil
}
