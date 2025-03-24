package connector

import "os"

func (s *Server) debugging() bool {
	return os.Getenv("SURREAL_FIVETRAN_DEBUG") != ""
}
