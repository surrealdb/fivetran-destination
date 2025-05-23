package connector

import (
	"fmt"
	"strings"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/connection"
)

func (s *Server) defineTable(db *surrealdb.DB, table *pb.Table) error {
	type TableData struct {
		Table string `json:"table"`
	}
	var ver connection.RPCResponse[TableData]
	if err := validateTableName(table.Name); err != nil {
		return err
	}
	tb := table.Name
	// if len(table.Columns) > 0 {
	query := fmt.Sprintf(`DEFINE TABLE IF NOT EXISTS %s SCHEMAFULL;`, tb)
	// } else {
	// 	query = fmt.Sprintf(`DEFINE TABLE IF NOT EXISTS %s SCHEMALESS;`, tb)
	// }
	if err := db.Send(&ver, "query", query); err != nil {
		return err
	}

	s.logInfo("Defined table", "table", tb, "query", query)

	var historyMode bool

	for _, c := range table.Columns {
		if c.Name == "id" {
			if s.debugging() {
				s.logDebug("Skipping id")
			}
			continue
		}

		// We assume the table will be written using WriteHistoryBatch
		// if the table has a _fivetran_start column.
		if c.Name == "_fivetran_start" {
			historyMode = true
		}

		if err := validateColumnName(c.Name); err != nil {
			return err
		}
		q, err := s.defineFieldQueryFromFt(tb, c)
		if err != nil {
			return err
		}
		if err := db.Send(&ver, "query", q); err != nil {
			return err
		}
		if s.debugging() {
			s.logDebug("Defined field", "field", c.Name, "table", tb, "query", q)
		}
	}

	if historyMode {
		q, err := s.defineFivetranStartFieldIndex(tb)
		if err != nil {
			return err
		}
		if err := db.Send(&ver, "query", q); err != nil {
			return err
		}
		if s.debugging() {
			s.logDebug("Defined fivetran_start field index", "table", tb, "query", q)
		}

		q, err = s.defineFivetranPKIndex(table)
		if err != nil {
			return err
		}
		if err := db.Send(&ver, "query", q); err != nil {
			return err
		}
		if s.debugging() {
			s.logDebug("Defined pkcol index", "table", tb, "query", q)
		}
	}
	return nil
}

func (s *Server) defineFivetranStartFieldIndex(tb string) (string, error) {
	return fmt.Sprintf(`DEFINE INDEX %s ON %s FIELDS _fivetran_start;`, tb, tb), nil
}

func (s *Server) defineFivetranPKIndex(table *pb.Table) (string, error) {
	var pkFields []string
	for _, c := range table.Columns {
		if c.PrimaryKey {
			pkFields = append(pkFields, c.Name)
		}
	}
	if len(pkFields) == 0 {
		return "", fmt.Errorf("no primary key columns found for table %s", table.Name)
	}
	return fmt.Sprintf(`DEFINE INDEX %s_pkcol ON %s FIELDS %s;`, table.Name, table.Name, strings.Join(pkFields, ", ")), nil
}
