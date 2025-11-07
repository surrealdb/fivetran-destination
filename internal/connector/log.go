package connector

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/surrealdb/fivetran-destination/internal/connector/log"
)

func LoggerFromEnv() (zerolog.Logger, error) {
	level := zerolog.InfoLevel
	if os.Getenv("SURREAL_FIVETRAN_DEBUG") != "" {
		level = zerolog.DebugLevel
	}
	return log.InitLogger(nil, level), nil
}
