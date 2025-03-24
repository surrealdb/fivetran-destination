package connector

import (
	"fmt"
	"log"

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
	var query string
	// if len(table.Columns) > 0 {
	query = fmt.Sprintf(`DEFINE TABLE IF NOT EXISTS %s SCHEMAFULL;`, tb)
	// } else {
	// 	query = fmt.Sprintf(`DEFINE TABLE IF NOT EXISTS %s SCHEMALESS;`, tb)
	// }
	if err := db.Send(&ver, "query", query); err != nil {
		return err
	}

	log.Printf("Defined table %s: %s", tb, query)

	for _, c := range table.Columns {
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

		log.Printf("Defined field %s for table %s: %s", c.Name, tb, q)
	}

	return nil
}
