package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // Add pprof HTTP endpoints
	"os"

	"github.com/surrealdb/fivetran-destination/internal/connector"
	_ "google.golang.org/grpc/encoding/gzip" // Register the gzip compressor
)

var (
	port      = flag.Int("port", 50052, "The server port")
	pprofPort = flag.Int("pprof-port", 6060, "The pprof server port")
)

func main() {
	logger, err := connector.LoggerFromEnv()
	if err != nil {
		log, jsonErr := json.Marshal(map[string]interface{}{
			"level":          "SEVERE",
			"message":        fmt.Sprintf("failed to create logger: %v", err),
			"message-origin": "sdk_destination",
		})
		if jsonErr != nil {
			panic(fmt.Errorf("unable to marshal error %q due to %q", err, jsonErr))
		}
		_, err := fmt.Fprintf(os.Stdout, "%s", log)
		if err != nil {
			panic(fmt.Errorf("unable to write error %q due to %q", err, err))
		}
	}

	flag.Parse()

	if *pprofPort > 0 {
		go func() {
			logger.Info().Int("pprof-port", *pprofPort).Msg("Starting pprof server")
			if err := http.ListenAndServe(fmt.Sprintf(":%d", *pprofPort), nil); err != nil {
				logger.Error().Err(err).Msg("pprof server failed")
			}
		}()
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Error().Err(err).Msg("failed to listen")
		os.Exit(1)
	}

	logger.Info().Int("port", *port).Msg("Starting SurrealDB destination connector")
	if err := connector.Serve(lis, logger); err != nil {
		logger.Error().Err(err).Msg("failed to serve")
	}
}
