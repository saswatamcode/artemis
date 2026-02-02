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

	"github.com/oklog/run"
	"github.com/prometheus/common/promslog"
	psflag "github.com/prometheus/common/promslog/flag"
	"github.com/prometheus/common/version"

	"github.com/saswatamcode/artemis/pkg/api"
	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/compactor"
	"github.com/saswatamcode/artemis/pkg/otlp"
	"github.com/saswatamcode/artemis/pkg/tempo"
	"github.com/saswatamcode/artemis/pkg/tracedb"
)

func main() {
	// Database flags
	walDir := flag.String("wal-dir", "./data/wal", "Directory for WAL segments")
	walSegmentSize := flag.Int64("wal-segment-size", 128*1024*1024, "WAL segment size in bytes (default 128MB)")
	blocksDir := flag.String("blocks-dir", "./data/blocks", "Directory for persisted blocks")
	compactInterval := flag.Duration("compact-interval", 10*time.Second, "How often to flush pending data to Arrow batches")
	checkpointInterval := flag.Duration("checkpoint-interval", 60*time.Second, "How often to create WAL checkpoints")
	checkpointThreshold := flag.Int("checkpoint-threshold", 5, "Create checkpoint after N segments")
	blockCompactionInterval := flag.Duration("block-compaction-interval", 5*time.Minute, "How often to run block compaction")
	retentionPeriod := flag.Duration("retention-period", 0, "Delete blocks older than this (0 = no retention)")
	enableCompaction := flag.Bool("enable-compaction", true, "Enable automatic block compaction")
	enableRetention := flag.Bool("enable-retention", false, "Enable automatic retention cleanup")

	// Block configuration flags
	maxBlockDuration := flag.Duration("max-block-duration", 2*time.Hour, "Maximum time range per block")
	maxBlockSpans := flag.Int64("max-block-spans", 1000000, "Maximum spans per block")

	// Compaction level configuration (for aggressive demo/testing)
	minBlockAge0 := flag.Duration("min-block-age-l0", 10*time.Minute, "Minimum age before compacting L0 blocks")
	minBlocks0 := flag.Int("min-blocks-l0", 2, "Minimum L0 blocks to trigger compaction")
	minBlockAge1 := flag.Duration("min-block-age-l1", 2*time.Hour, "Minimum age before compacting L1 blocks")
	minBlocks1 := flag.Int("min-blocks-l1", 2, "Minimum L1 blocks to trigger compaction")

	// Server address flags
	otlpAddr := flag.String("otlp-addr", ":4317", "OTLP gRPC receiver address")
	apiAddr := flag.String("api-addr", ":16686", "HTTP API (Jaeger) address")
	tempoAddr := flag.String("tempo-addr", ":3200", "Tempo API address")

	// Logging flags
	logLevelStr := flag.String("log.level", "info", psflag.LevelFlagHelp)
	logFormatStr := flag.String("log.format", "logfmt", psflag.FormatFlagHelp)

	// Version flag
	showVersion := flag.Bool("version", false, "Show version information and exit")

	flag.Parse()

	// Handle version flag
	if *showVersion {
		fmt.Printf("Artemis Trace Server\n")
		fmt.Printf("Version:    %s\n", version.Version)
		fmt.Printf("Revision:   %s\n", version.Revision)
		fmt.Printf("Branch:     %s\n", version.Branch)
		fmt.Printf("Build Date: %s\n", version.BuildDate)
		fmt.Printf("Build User: %s\n", version.BuildUser)
		fmt.Printf("Go Version: %s\n", version.GoVersion)
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

	apiServer := api.NewServer(db, logger)
	tempoServer := tempo.NewServer(db, logger)

	// Print server info
	logger.Info("artemis server starting",
		"otlp_addr", *otlpAddr,
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
