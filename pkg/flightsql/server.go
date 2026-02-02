package flightsql

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"

	"github.com/saswatamcode/artemis/pkg/tracedb"
)

// Server is a gRPC server that implements Flight SQL
type Server struct {
	service    flightsql.Server
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *slog.Logger
}

// NewServer creates a new Flight SQL gRPC server
func NewServer(db *tracedb.DB, addr string, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	// Create the Flight SQL service implementation
	service := NewFlightSQLService(db, logger)

	// Create gRPC server and register Flight SQL service
	grpcServer := grpc.NewServer()
	flightServer := flightsql.NewFlightServer(service)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)

	s := &Server{
		service:    service,
		grpcServer: grpcServer,
		listener:   listener,
		logger:     logger,
	}

	return s, nil
}

// Start starts the gRPC server (blocking)
func (s *Server) Start() error {
	s.logger.Info("Flight SQL server listening", "addr", s.listener.Addr().String())
	return s.grpcServer.Serve(s.listener)
}

// Stop gracefully stops the server
func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}

// Addr returns the server's listen address
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}
