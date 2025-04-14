package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/surrealdb/fivetran-destination/internal/connector"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Register the gzip compressor
)

var (
	port = flag.Int("port", 50052, "The server port")
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
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Error().Err(err).Msg("failed to listen")
	}

	// Create a new gRPC server with increased message size limits
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*50), // 50MB
		grpc.MaxSendMsgSize(1024*1024*50), // 50MB
	)
	srv := connector.NewServer(logger)

	pb.RegisterDestinationConnectorServer(s, srv)

	logger.Info().Int("port", *port).Msg("Starting SurrealDB destination connector")
	if err := s.Serve(lis); err != nil {
		logger.Error().Err(err).Msg("failed to serve")
	}
}
