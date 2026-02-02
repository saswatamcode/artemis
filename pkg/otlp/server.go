package otlp

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/saswatamcode/artemis/pkg/tracedb"
)

// Server is a gRPC server that implements OTLP trace service
type Server struct {
	coltracev1.UnimplementedTraceServiceServer
	receiver   *Receiver
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *slog.Logger
}

// NewServer creates a new OTLP gRPC server
func NewServer(db *tracedb.DB, addr string, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	s := &Server{
		receiver:   NewReceiver(db),
		grpcServer: grpc.NewServer(),
		listener:   listener,
		logger:     logger,
	}

	coltracev1.RegisterTraceServiceServer(s.grpcServer, s)
	return s, nil
}

// Export implements the OTLP TraceService Export RPC
func (s *Server) Export(ctx context.Context, req *coltracev1.ExportTraceServiceRequest) (*coltracev1.ExportTraceServiceResponse, error) {
	return s.receiver.Export(ctx, req)
}

// Start starts the gRPC server
func (s *Server) Start() error {
	s.logger.Info("OTLP receiver listening", "addr", s.listener.Addr().String())
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
