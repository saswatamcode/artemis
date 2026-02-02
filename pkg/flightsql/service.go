package flightsql

import (
	"context"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/saswatamcode/artemis/pkg/tracedb"
)

// FlightSQLService implements the Flight SQL server
// Embeds BaseServer to get default implementations
type FlightSQLService struct {
	flightsql.BaseServer
	db       *tracedb.DB
	executor *SQLExecutor
	logger   *slog.Logger
	mem      memory.Allocator
}

// NewFlightSQLService creates a new Flight SQL service
func NewFlightSQLService(db *tracedb.DB, logger *slog.Logger) flightsql.Server {
	if logger == nil {
		logger = slog.Default()
	}

	service := &FlightSQLService{
		db:       db,
		executor: NewSQLExecutor(db, logger),
		logger:   logger,
		mem:      memory.NewGoAllocator(),
	}

	return service
}

// GetFlightInfoStatement returns metadata about a SQL query
func (s *FlightSQLService) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s.logger.Debug("GetFlightInfoStatement", "query", cmd.GetQuery())

	// Create a ticket for this query
	ticket, err := flightsql.CreateStatementQueryTicket([]byte(cmd.GetQuery()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create ticket: %v", err)
	}

	// Return flight info with schema
	schema := GetSpansSchema()
	endpoint := &flight.FlightEndpoint{
		Ticket: &flight.Ticket{Ticket: ticket},
	}

	info := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.mem),
		FlightDescriptor: desc,
		Endpoint:         []*flight.FlightEndpoint{endpoint},
		TotalRecords:     -1, // Unknown until executed
		TotalBytes:       -1,
	}

	return info, nil
}

// DoGetStatement executes a SQL query and streams results
func (s *FlightSQLService) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := string(cmd.GetStatementHandle())
	s.logger.Debug("DoGetStatement", "query", query)

	// Execute query
	record, err := s.executor.Execute(query)
	if err != nil {
		s.logger.Error("failed to execute query", "error", err, "query", query)
		return nil, nil, status.Errorf(codes.Internal, "failed to execute query: %v", err)
	}

	// Stream the record
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{
		Data: record,
		Desc: nil,
		Err:  nil,
	}
	close(ch)

	return GetSpansSchema(), ch, nil
}

// GetSchemaStatement returns the schema for a SQL query
func (s *FlightSQLService) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	s.logger.Debug("GetSchemaStatement", "query", cmd.GetQuery())

	schema := GetSpansSchema()
	return &flight.SchemaResult{
		Schema: flight.SerializeSchema(schema, s.mem),
	}, nil
}

// CreatePreparedStatement creates a prepared statement (not implemented yet)
func (s *FlightSQLService) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	return flightsql.ActionCreatePreparedStatementResult{}, status.Errorf(codes.Unimplemented, "prepared statements not yet supported")
}

// ClosePreparedStatement closes a prepared statement (not implemented yet)
func (s *FlightSQLService) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	return status.Errorf(codes.Unimplemented, "prepared statements not yet supported")
}

// DoPutPreparedStatementQuery binds parameters (not implemented yet)
func (s *FlightSQLService) DoPutPreparedStatementQuery(ctx context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, wrt flight.MetadataWriter) ([]byte, error) {
	return nil, status.Errorf(codes.Unimplemented, "prepared statements not yet supported")
}

// GetFlightInfoPreparedStatement returns metadata for prepared statement (not implemented yet)
func (s *FlightSQLService) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "prepared statements not yet supported")
}
