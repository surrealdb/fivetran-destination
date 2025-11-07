package connector

import (
	"context"
	"net"

	"github.com/rs/zerolog"
	"github.com/surrealdb/fivetran-destination/internal/connector/server"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"google.golang.org/grpc"
)

func Serve(lis net.Listener, logger zerolog.Logger) error {
	// Create a new gRPC server with increased message size limits
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*50), // 50MB
		grpc.MaxSendMsgSize(1024*1024*50), // 50MB
	)
	srv := server.New(logger)

	// Start server components (metrics collector, etc.)
	ctx := context.Background()
	srv.Start(ctx)

	pb.RegisterDestinationConnectorServer(s, srv)

	return s.Serve(lis)
}
