package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"regexp"
	"syscall"
	"time"

	"github.com/felixge/fgprof"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promslog"
	psflag "github.com/prometheus/common/promslog/flag"
	"github.com/prometheus/common/version"
	"github.com/spf13/cobra"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/compactor"
	"github.com/saswatamcode/artemis/pkg/jaeger"
	"github.com/saswatamcode/artemis/pkg/metrics"
	"github.com/saswatamcode/artemis/pkg/otlp"
	"github.com/saswatamcode/artemis/pkg/queryapi"
	"github.com/saswatamcode/artemis/pkg/sqlapi"
	"github.com/saswatamcode/artemis/pkg/tempo"
	"github.com/saswatamcode/artemis/pkg/tracedb"
)

var (
	// Database flags
	walDir                  string
	walSegmentSize          int64
	blocksDir               string
	compactInterval         time.Duration
	checkpointInterval      time.Duration
	checkpointThreshold     int
	blockCompactionInterval time.Duration
	retentionPeriod         time.Duration
	enableCompaction        bool
	enableRetention         bool

	// Block configuration flags
	maxBlockDuration time.Duration
	maxBlockSpans    int64

	// Compaction level configuration
	minBlockAge0 time.Duration
	minBlocks0   int
	minBlockAge1 time.Duration
	minBlocks1   int

	// Server address flags
	otlpAddr     string
	jaegerAddr   string
	tempoAddr    string
	sqlAPIAddr   string
	queryAPIAddr string
	profileAddr  string
	metricsAddr  string

	// Logging flags
	logLevelStr  string
	logFormatStr string

	// Root command
	rootCmd = &cobra.Command{
		Use:   "artemis",
		Short: "Artemis Trace Server",
		Long:  `Artemis is a high-performance trace storage and query backend.`,
		RunE:  runServer,
	}

	// Version command
	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Artemis Trace Server\n")
			fmt.Printf("Version:    %s\n", version.Version)
			fmt.Printf("Revision:   %s\n", version.Revision)
			fmt.Printf("Branch:     %s\n", version.Branch)
			fmt.Printf("Build Date: %s\n", version.BuildDate)
			fmt.Printf("Build User: %s\n", version.BuildUser)
			fmt.Printf("Go Version: %s\n", version.GoVersion)
		},
	}
)

func init() {
	// Add version command
	rootCmd.AddCommand(versionCmd)

	// Database flags
	rootCmd.Flags().StringVar(&walDir, "wal-dir", "./data/wal", "Directory for WAL segments")
	rootCmd.Flags().Int64Var(&walSegmentSize, "wal-segment-size", 128*1024*1024, "WAL segment size in bytes (default 128MB)")
	rootCmd.Flags().StringVar(&blocksDir, "blocks-dir", "./data/blocks", "Directory for persisted blocks")
	rootCmd.Flags().DurationVar(&compactInterval, "compact-interval", 10*time.Second, "How often to flush pending data to Arrow batches")
	rootCmd.Flags().DurationVar(&checkpointInterval, "checkpoint-interval", 60*time.Second, "How often to create WAL checkpoints")
	rootCmd.Flags().IntVar(&checkpointThreshold, "checkpoint-threshold", 5, "Create checkpoint after N segments")
	rootCmd.Flags().DurationVar(&blockCompactionInterval, "block-compaction-interval", 5*time.Minute, "How often to run block compaction")
	rootCmd.Flags().DurationVar(&retentionPeriod, "retention-period", 0, "Delete blocks older than this (0 = no retention)")
	rootCmd.Flags().BoolVar(&enableCompaction, "enable-compaction", true, "Enable automatic block compaction")
	rootCmd.Flags().BoolVar(&enableRetention, "enable-retention", false, "Enable automatic retention cleanup")

	// Block configuration flags
	rootCmd.Flags().DurationVar(&maxBlockDuration, "max-block-duration", 2*time.Hour, "Maximum time range per block")
	rootCmd.Flags().Int64Var(&maxBlockSpans, "max-block-spans", 1000000, "Maximum spans per block")

	// Compaction level configuration (for aggressive demo/testing)
	rootCmd.Flags().DurationVar(&minBlockAge0, "min-block-age-l0", 10*time.Minute, "Minimum age before compacting L0 blocks")
	rootCmd.Flags().IntVar(&minBlocks0, "min-blocks-l0", 2, "Minimum L0 blocks to trigger compaction")
	rootCmd.Flags().DurationVar(&minBlockAge1, "min-block-age-l1", 2*time.Hour, "Minimum age before compacting L1 blocks")
	rootCmd.Flags().IntVar(&minBlocks1, "min-blocks-l1", 2, "Minimum L1 blocks to trigger compaction")

	// Server address flags
	rootCmd.Flags().StringVar(&otlpAddr, "otlp-addr", ":4317", "OTLP gRPC receiver address")
	rootCmd.Flags().StringVar(&jaegerAddr, "jaeger-addr", ":16686", "HTTP API (Jaeger) address")
	rootCmd.Flags().StringVar(&tempoAddr, "tempo-addr", ":3200", "Tempo API address")
	rootCmd.Flags().StringVar(&sqlAPIAddr, "sqlapi-addr", ":5433", "SQL API address")
	rootCmd.Flags().StringVar(&queryAPIAddr, "queryapi-addr", ":8080", "Query API and Web UI address")
	rootCmd.Flags().StringVar(&profileAddr, "profile-addr", ":6060", "pprof profiling server address; empty disables")
	rootCmd.Flags().StringVar(&metricsAddr, "metrics-addr", ":9090", "Prometheus metrics server address")

	// Logging flags
	rootCmd.Flags().StringVar(&logLevelStr, "log.level", "info", psflag.LevelFlagHelp)
	rootCmd.Flags().StringVar(&logFormatStr, "log.format", "logfmt", psflag.FormatFlagHelp)
}

func runServer(cmd *cobra.Command, args []string) error {
	logLevel := promslog.NewLevel()
	if err := logLevel.Set(logLevelStr); err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}

	logFormat := promslog.NewFormat()
	if err := logFormat.Set(logFormatStr); err != nil {
		return fmt.Errorf("invalid log format: %w", err)
	}

	logger := promslog.New(&promslog.Config{
		Level:  logLevel,
		Format: logFormat,
		Style:  promslog.GoKitStyle,
		Writer: os.Stderr,
	})

	slog.SetDefault(logger)

	logger.Info("starting artemis", "build_info", version.Info(), "build_context", version.BuildContext())

	// Create Prometheus metrics registry
	metricsRegistry := prometheus.NewRegistry()
	metricsRegistry.MustRegister(
		collectors.NewBuildInfoCollector(),
		collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(
				collectors.GoRuntimeMetricsRule{Matcher: regexp.MustCompile("/.*")},
			),
		),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	// Create custom metrics
	dbMetrics := metrics.NewDatabaseMetrics(metricsRegistry)
	apiMetrics := metrics.NewAPIMetrics(metricsRegistry)

	// Build custom compaction level configs
	levelConfigs := compactor.DefaultLevelConfigs()

	// Override L0 and L1 with flags (if not default values)
	if minBlockAge0 != 10*time.Minute || minBlocks0 != 2 {
		levelConfigs[0].MinBlockAge = minBlockAge0
		levelConfigs[0].MinBlocks = minBlocks0
	}
	if minBlockAge1 != 2*time.Hour || minBlocks1 != 2 {
		levelConfigs[1].MinBlockAge = minBlockAge1
		levelConfigs[1].MinBlocks = minBlocks1
	}

	// Configure database
	cfg := &tracedb.Config{
		WALDir:                  walDir,
		WALSegmentSize:          walSegmentSize,
		CompactInterval:         compactInterval,
		CheckpointInterval:      checkpointInterval,
		CheckpointThreshold:     checkpointThreshold,
		BlockCompactionInterval: blockCompactionInterval,
		RetentionPeriod:         retentionPeriod,
		CompactionLevels:        levelConfigs,
		EnableCompaction:        enableCompaction,
		EnableRetention:         enableRetention,
		Logger:                  logger,
		Metrics:                 dbMetrics,
		BlockConfig: &block.Config{
			Dir:              blocksDir,
			MaxBlockDuration: maxBlockDuration,
			MaxBlockSpans:    maxBlockSpans,
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
	otlpServer, err := otlp.NewServer(db, otlpAddr, logger, dbMetrics, apiMetrics)
	if err != nil {
		log.Fatalf("Failed to create OTLP server: %v", err)
	}

	jaegerServer := jaeger.NewServer(db, logger, dbMetrics, apiMetrics)
	tempoServer := tempo.NewServer(db, logger, dbMetrics, apiMetrics)
	sqlAPIServer := sqlapi.NewServer(db, logger, dbMetrics, apiMetrics)
	queryAPIServer := queryapi.NewServer(db, logger, dbMetrics, apiMetrics)

	// Print server info
	logger.Info("artemis server starting",
		"otlp_addr", otlpAddr,
		"jaeger_api", "http://localhost"+jaegerAddr,
		"tempo_api", "http://localhost"+tempoAddr,
		"sqlapi", "http://localhost"+sqlAPIAddr,
		"queryapi", "http://localhost"+queryAPIAddr,
		"web_ui", "http://localhost"+queryAPIAddr,
	)
	logger.Info("grafana configuration",
		"jaeger_type", "Jaeger",
		"jaeger_url", "http://localhost"+jaegerAddr,
		"tempo_type", "Tempo",
		"tempo_url", "http://localhost"+tempoAddr,
	)
	logger.Info("available endpoints",
		"web_ui", queryAPIAddr+"/",
		"jaeger_trace", jaegerAddr+"/api/traces/{traceID}",
		"jaeger_search", jaegerAddr+"/api/traces?service=...",
		"jaeger_services", jaegerAddr+"/api/services",
		"jaeger_health", jaegerAddr+"/health",
		"tempo_search_v1", tempoAddr+"/api/search",
		"tempo_search_v2", tempoAddr+"/api/v2/search",
		"tempo_trace", tempoAddr+"/api/traces/{traceID}",
		"tempo_tags_v2", tempoAddr+"/api/v2/search/tags",
		"tempo_tag_values_v2", tempoAddr+"/api/v2/search/tag/{tag}/values",
		"tempo_buildinfo", tempoAddr+"/api/status/buildinfo",
		"sqlapi_query", sqlAPIAddr+"/api/query",
		"sqlapi_health", sqlAPIAddr+"/health",
		"queryapi_attr_keys", queryAPIAddr+"/api/v1/metadata/attribute_keys",
		"queryapi_attr_values", queryAPIAddr+"/api/v1/metadata/attribute_values?key={key}",
		"queryapi_query_range", queryAPIAddr+"/api/v1/query_range",
		"queryapi_trace", queryAPIAddr+"/api/v1/query/trace?traceID={traceID}",
		"queryapi_health", queryAPIAddr+"/api/v1/health",
	)
	if profileAddr != "" {
		logger.Info("profiling enabled", "addr", profileAddr, "pprof", "http://localhost"+profileAddr+"/debug/pprof/")
	}
	if metricsAddr != "" {
		logger.Info("metrics enabled", "addr", metricsAddr, "endpoint", "http://localhost"+metricsAddr+"/metrics")
	}
	logger.Info("press ctrl+c to shutdown")

	// Setup run.Group for coordinated startup/shutdown
	g := &run.Group{}

	// OTLP server actor
	g.Add(func() error {
		logger.Info("starting otlp receiver", "addr", otlpAddr)
		return otlpServer.Start()
	}, func(error) {
		logger.Info("stopping otlp receiver")
		otlpServer.Stop()
	})

	// Jaeger API server actor
	g.Add(func() error {
		logger.Info("starting http api (jaeger)", "addr", jaegerAddr)
		return jaegerServer.Start(jaegerAddr)
	}, func(error) {
		if err := jaegerServer.Shutdown(); err != nil {
			logger.Error("failed to shutdown api server", "error", err)
		}
	})

	// Tempo server actor
	g.Add(func() error {
		logger.Info("starting tempo api", "addr", tempoAddr)
		return tempoServer.Start(tempoAddr)
	}, func(error) {
		if err := tempoServer.Shutdown(); err != nil {
			logger.Error("failed to shutdown tempo server", "error", err)
		}
	})

	// SQL API server actor
	g.Add(func() error {
		logger.Info("starting sql api", "addr", sqlAPIAddr)
		return sqlAPIServer.Start(sqlAPIAddr)
	}, func(error) {
		if err := sqlAPIServer.Shutdown(); err != nil {
			logger.Error("failed to shutdown sql api server", "error", err)
		}
	})

	// Query API server actor
	g.Add(func() error {
		logger.Info("starting query api", "addr", queryAPIAddr)
		return queryAPIServer.Start(queryAPIAddr)
	}, func(error) {
		if err := queryAPIServer.Shutdown(); err != nil {
			logger.Error("failed to shutdown query api server", "error", err)
		}
	})

	// Pprof server actor
	if profileAddr != "" {
		profileMux := http.NewServeMux()
		profileMux.HandleFunc("/debug/pprof/", pprof.Index)
		profileMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		profileMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		profileMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		profileMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		profileMux.Handle("/debug/fgprof", fgprof.Handler())
		profileServer := &http.Server{Addr: profileAddr, Handler: profileMux}
		g.Add(func() error {
			logger.Info("starting profiling server", "addr", profileAddr)
			return profileServer.ListenAndServe()
		}, func(error) {
			if err := profileServer.Shutdown(context.Background()); err != nil && err != http.ErrServerClosed {
				logger.Error("failed to shutdown profiling server", "error", err)
			}
		})
	}

	// Metrics server actor
	if metricsAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}))
		metricsServer := &http.Server{Addr: metricsAddr, Handler: metricsMux}
		g.Add(func() error {
			logger.Info("starting metrics server", "addr", metricsAddr)
			return metricsServer.ListenAndServe()
		}, func(error) {
			if err := metricsServer.Shutdown(context.Background()); err != nil && err != http.ErrServerClosed {
				logger.Error("failed to shutdown metrics server", "error", err)
			}
		})
	}

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
	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
