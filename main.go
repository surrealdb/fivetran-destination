package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/surrealdb/fivetran-destination/internal/connector"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Register the gzip compressor
)

var (
	port = flag.Int("port", 50052, "The server port")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create a new gRPC server with increased message size limits
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*50), // 50MB
		grpc.MaxSendMsgSize(1024*1024*50), // 50MB
	)
	srv := connector.NewServer()

	pb.RegisterDestinationConnectorServer(s, srv)

	log.Printf("Starting SurrealDB destination connector on port %d", *port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
