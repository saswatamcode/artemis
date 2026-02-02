package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowflightsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/oklog/run"
	"github.com/prometheus/common/promslog"
	psflag "github.com/prometheus/common/promslog/flag"
	"github.com/prometheus/common/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/saswatamcode/artemis/pkg/api"
	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/compactor"
	"github.com/saswatamcode/artemis/pkg/flightsql"
	"github.com/saswatamcode/artemis/pkg/otlp"
	"github.com/saswatamcode/artemis/pkg/tempo"
	"github.com/saswatamcode/artemis/pkg/tracedb"
)

func main() {
	// Check for subcommand
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "query":
			runQuery(os.Args[2:])
			return
		case "server":
			runServer(os.Args[2:])
			return
		case "-h", "--help", "help":
			printUsage()
			return
		case "-version", "--version":
			printVersion()
			return
		}
	}

	// Default to server if no subcommand
	runServer(os.Args[1:])
}

func printUsage() {
	fmt.Println("Artemis - Distributed Tracing Database")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  artemis [command] [flags]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  server    Start the Artemis server (default)")
	fmt.Println("  query     Execute a SQL query against Flight SQL")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  # Start server")
	fmt.Println("  artemis server --flight-addr=:8815")
	fmt.Println()
	fmt.Println("  # Query spans")
	fmt.Println("  artemis query \"SELECT * FROM spans LIMIT 10\"")
	fmt.Println()
	fmt.Println("Use 'artemis [command] -h' for more information about a command.")
}

func printVersion() {
	fmt.Printf("Artemis Trace Server\n")
	fmt.Printf("Version:    %s\n", version.Version)
	fmt.Printf("Revision:   %s\n", version.Revision)
	fmt.Printf("Branch:     %s\n", version.Branch)
	fmt.Printf("Build Date: %s\n", version.BuildDate)
	fmt.Printf("Build User: %s\n", version.BuildUser)
	fmt.Printf("Go Version: %s\n", version.GoVersion)
}

func runQuery(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	addr := fs.String("addr", "localhost:8815", "Flight SQL server address")
	displayLimit := fs.Int("limit", 100, "Maximum rows to display")

	fs.Usage = func() {
		fmt.Println("Usage: artemis query [flags] <SQL>")
		fmt.Println()
		fmt.Println("Execute a SQL query against Artemis Flight SQL server")
		fmt.Println()
		fmt.Println("Flags:")
		fs.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  artemis query \"SELECT * FROM spans\"")
		fmt.Println("  artemis query \"SELECT * FROM spans WHERE service_name = 'api' LIMIT 10\"")
		fmt.Println("  artemis query \"SELECT trace_id, duration FROM spans ORDER BY duration DESC LIMIT 5\"")
	}

	fs.Parse(args)

	if fs.NArg() == 0 {
		fmt.Println("Error: SQL query required")
		fmt.Println()
		fs.Usage()
		os.Exit(1)
	}

	query := fs.Arg(0)

	// Connect to Flight SQL with insecure credentials
	client, err := arrowflightsql.NewClient(*addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", *addr, err)
	}
	defer client.Close()

	fmt.Printf("Connected to %s\n", *addr)
	fmt.Printf("Executing: %s\n\n", query)

	ctx := context.Background()

	// Execute query
	info, err := client.Execute(ctx, query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}

	if len(info.Endpoint) == 0 {
		fmt.Println("No results returned")
		return
	}

	// Read results
	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		log.Fatalf("Failed to get results: %v", err)
	}
	defer reader.Release()

	totalRows := 0
	rowsDisplayed := 0

	// Process records
	for reader.Next() {
		record := reader.Record()
		numRows := int(record.NumRows())
		totalRows += numRows

		// Print header on first record
		if rowsDisplayed == 0 {
			printHeader(record.Schema())
		}

		// Print rows
		for i := 0; i < numRows && rowsDisplayed < *displayLimit; i++ {
			printRow(record, i)
			rowsDisplayed++
		}

		if rowsDisplayed >= *displayLimit {
			break
		}
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Reader error: %v", err)
	}

	fmt.Printf("\n---\n")
	fmt.Printf("Total rows: %d", totalRows)
	if rowsDisplayed < totalRows {
		fmt.Printf(" (showing first %d)", rowsDisplayed)
	}
	fmt.Println()
}

func printHeader(schema *arrow.Schema) {
	fmt.Print("| ")
	for _, field := range schema.Fields() {
		fmt.Printf("%-20s | ", field.Name)
	}
	fmt.Println()

	fmt.Print("|-")
	for range schema.Fields() {
		fmt.Print("---------------------|-")
	}
	fmt.Println()
}

func printRow(record arrow.Record, rowIdx int) {
	fmt.Print("| ")
	for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
		col := record.Column(colIdx)
		value := formatValue(col, rowIdx)
		fmt.Printf("%-20s | ", truncate(value, 20))
	}
	fmt.Println()
}

func formatValue(arr arrow.Array, idx int) string {
	if arr.IsNull(idx) {
		return "<null>"
	}

	switch arr := arr.(type) {
	case *array.String:
		return arr.Value(idx)
	case *array.Int64:
		val := arr.Value(idx)
		// Convert duration to milliseconds for readability
		if val > 1000000 {
			return fmt.Sprintf("%d (%.2fms)", val, float64(val)/1e6)
		}
		return fmt.Sprintf("%d", val)
	case *array.Map:
		// For map columns (tags), just indicate presence
		if arr.IsNull(idx) {
			return "<no tags>"
		}
		return "<tags>"
	default:
		return fmt.Sprintf("%v", arr)
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)

	// Database flags
	walDir := fs.String("wal-dir", "./data/wal", "Directory for WAL segments")
	walSegmentSize := fs.Int64("wal-segment-size", 128*1024*1024, "WAL segment size in bytes (default 128MB)")
	blocksDir := fs.String("blocks-dir", "./data/blocks", "Directory for persisted blocks")
	compactInterval := fs.Duration("compact-interval", 10*time.Second, "How often to flush pending data to Arrow batches")
	checkpointInterval := fs.Duration("checkpoint-interval", 60*time.Second, "How often to create WAL checkpoints")
	checkpointThreshold := fs.Int("checkpoint-threshold", 5, "Create checkpoint after N segments")
	blockCompactionInterval := fs.Duration("block-compaction-interval", 5*time.Minute, "How often to run block compaction")
	retentionPeriod := fs.Duration("retention-period", 0, "Delete blocks older than this (0 = no retention)")
	enableCompaction := fs.Bool("enable-compaction", true, "Enable automatic block compaction")
	enableRetention := fs.Bool("enable-retention", false, "Enable automatic retention cleanup")

	// Block configuration flags
	maxBlockDuration := fs.Duration("max-block-duration", 2*time.Hour, "Maximum time range per block")
	maxBlockSpans := fs.Int64("max-block-spans", 1000000, "Maximum spans per block")

	// Compaction level configuration (for aggressive demo/testing)
	minBlockAge0 := fs.Duration("min-block-age-l0", 10*time.Minute, "Minimum age before compacting L0 blocks")
	minBlocks0 := fs.Int("min-blocks-l0", 2, "Minimum L0 blocks to trigger compaction")
	minBlockAge1 := fs.Duration("min-block-age-l1", 2*time.Hour, "Minimum age before compacting L1 blocks")
	minBlocks1 := fs.Int("min-blocks-l1", 2, "Minimum L1 blocks to trigger compaction")

	// Server address flags
	otlpAddr := fs.String("otlp-addr", ":4317", "OTLP gRPC receiver address")
	apiAddr := fs.String("api-addr", ":16686", "HTTP API (Jaeger) address")
	tempoAddr := fs.String("tempo-addr", ":3200", "Tempo API address")
	flightAddr := fs.String("flight-addr", ":8815", "Flight SQL address")

	// Logging flags
	logLevelStr := fs.String("log.level", "info", psflag.LevelFlagHelp)
	logFormatStr := fs.String("log.format", "logfmt", psflag.FormatFlagHelp)

	// Version flag
	showVersion := fs.Bool("version", false, "Show version information and exit")

	fs.Parse(args)

	// Handle version flag
	if *showVersion {
		printVersion()
		os.Exit(0)
	}

	logLevel := promslog.NewLevel()
	if err := logLevel.Set(*logLevelStr); err != nil {
		os.Exit(1)
	}

	logFormat := promslog.NewFormat()
	if err := logFormat.Set(*logFormatStr); err != nil {
		os.Exit(1)
	}

	logger := promslog.New(&promslog.Config{
		Level:  logLevel,
		Format: logFormat,
		Style:  promslog.GoKitStyle,
		Writer: os.Stderr,
	})

	slog.SetDefault(logger)

	logger.Info("starting artemis", "build_info", version.Info(), "build_context", version.BuildContext())

	// Build custom compaction level configs
	levelConfigs := compactor.DefaultLevelConfigs()

	// Override L0 and L1 with flags (if not default values)
	if *minBlockAge0 != 10*time.Minute || *minBlocks0 != 2 {
		levelConfigs[0].MinBlockAge = *minBlockAge0
		levelConfigs[0].MinBlocks = *minBlocks0
	}
	if *minBlockAge1 != 2*time.Hour || *minBlocks1 != 2 {
		levelConfigs[1].MinBlockAge = *minBlockAge1
		levelConfigs[1].MinBlocks = *minBlocks1
	}

	// Configure database
	cfg := &tracedb.Config{
		WALDir:                  *walDir,
		WALSegmentSize:          *walSegmentSize,
		CompactInterval:         *compactInterval,
		CheckpointInterval:      *checkpointInterval,
		CheckpointThreshold:     *checkpointThreshold,
		BlockCompactionInterval: *blockCompactionInterval,
		RetentionPeriod:         *retentionPeriod,
		CompactionLevels:        levelConfigs,
		EnableCompaction:        *enableCompaction,
		EnableRetention:         *enableRetention,
		Logger:                  logger,
		BlockConfig: &block.Config{
			Dir:              *blocksDir,
			MaxBlockDuration: *maxBlockDuration,
			MaxBlockSpans:    *maxBlockSpans,
			Logger:           logger,
		},
	}

	// Log configuration
	logger.Info("database configuration",
		"wal_dir", cfg.WALDir,
		"wal_segment_size", fmt.Sprintf("%dMB", cfg.WALSegmentSize/(1024*1024)),
		"blocks_dir", cfg.BlockConfig.Dir,
		"compact_interval", cfg.CompactInterval,
		"checkpoint_interval", cfg.CheckpointInterval,
		"checkpoint_threshold", cfg.CheckpointThreshold,
		"block_compaction_interval", cfg.BlockCompactionInterval,
		"retention_period", cfg.RetentionPeriod,
		"enable_compaction", cfg.EnableCompaction,
		"enable_retention", cfg.EnableRetention,
		"max_block_duration", cfg.BlockConfig.MaxBlockDuration,
		"max_block_spans", cfg.BlockConfig.MaxBlockSpans,
	)
	logger.Info("compaction level configuration",
		"l0_min_age", levelConfigs[0].MinBlockAge,
		"l0_min_blocks", levelConfigs[0].MinBlocks,
		"l1_min_age", levelConfigs[1].MinBlockAge,
		"l1_min_blocks", levelConfigs[1].MinBlocks,
	)

	// Open database
	db, err := tracedb.New(cfg)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	logger.Info("database opened", "wal_replay", "complete")

	// Create servers
	otlpServer, err := otlp.NewServer(db, *otlpAddr, logger)
	if err != nil {
		log.Fatalf("Failed to create OTLP server: %v", err)
	}

	flightServer, err := flightsql.NewServer(db, *flightAddr, logger)
	if err != nil {
		log.Fatalf("Failed to create Flight SQL server: %v", err)
	}

	apiServer := api.NewServer(db, logger)
	tempoServer := tempo.NewServer(db, logger)

	// Print server info
	logger.Info("artemis server starting",
		"otlp_addr", *otlpAddr,
		"flight_addr", *flightAddr,
		"jaeger_api", "http://localhost"+*apiAddr,
		"tempo_api", "http://localhost"+*tempoAddr,
	)
	logger.Info("grafana configuration",
		"jaeger_type", "Jaeger",
		"jaeger_url", "http://localhost"+*apiAddr,
		"tempo_type", "Tempo",
		"tempo_url", "http://localhost"+*tempoAddr,
	)
	logger.Info("available endpoints",
		"jaeger_trace", *apiAddr+"/api/traces/{traceID}",
		"jaeger_search", *apiAddr+"/api/traces?service=...",
		"jaeger_services", *apiAddr+"/api/services",
		"jaeger_health", *apiAddr+"/health",
		"tempo_search_v1", *tempoAddr+"/api/search",
		"tempo_search_v2", *tempoAddr+"/api/v2/search",
		"tempo_trace", *tempoAddr+"/api/traces/{traceID}",
		"tempo_tags_v2", *tempoAddr+"/api/v2/search/tags",
		"tempo_tag_values_v2", *tempoAddr+"/api/v2/search/tag/{tag}/values",
		"tempo_buildinfo", *tempoAddr+"/api/status/buildinfo",
	)
	logger.Info("press ctrl+c to shutdown")

	// Setup run.Group for coordinated startup/shutdown
	g := &run.Group{}

	// OTLP server actor
	g.Add(func() error {
		logger.Info("starting otlp receiver", "addr", *otlpAddr)
		return otlpServer.Start()
	}, func(error) {
		logger.Info("stopping otlp receiver")
		otlpServer.Stop()
	})

	// Flight SQL server actor
	g.Add(func() error {
		logger.Info("starting flight sql server", "addr", *flightAddr)
		return flightServer.Start()
	}, func(error) {
		logger.Info("stopping flight sql server")
		flightServer.Stop()
	})

	// API server actor
	g.Add(func() error {
		logger.Info("starting http api (jaeger)", "addr", *apiAddr)
		return apiServer.Start(*apiAddr)
	}, func(error) {
		if err := apiServer.Shutdown(); err != nil {
			logger.Error("failed to shutdown api server", "error", err)
		}
	})

	// Tempo server actor
	g.Add(func() error {
		logger.Info("starting tempo api", "addr", *tempoAddr)
		return tempoServer.Start(*tempoAddr)
	}, func(error) {
		if err := tempoServer.Shutdown(); err != nil {
			logger.Error("failed to shutdown tempo server", "error", err)
		}
	})

	// Signal handler actor
	g.Add(run.SignalHandler(context.Background(), syscall.SIGINT, syscall.SIGTERM))

	// Run all actors
	if err := g.Run(); err != nil {
		var sigErr run.SignalError
		if err == sigErr {
			logger.Info("received signal, shutting down gracefully")
		} else {
			logger.Error("server error", "error", err)
		}
	}

	// Database close (triggered by defer)
	logger.Info("shutdown complete")
}
