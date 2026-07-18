package cmd

import (
	"context"
	"data-migration/internal/config"
	"data-migration/internal/migration"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	cfg = &config.Config{}

	// rootCmd represents the base command
	rootCmd = &cobra.Command{
		Use:   "migrationdb",
		Short: "Database migration tool",
		Long: `A database migration tool that supports streaming data between different database systems.
Source and target databases must be of the same type (e.g., postgres to postgres, mysql to mysql).

Supported connection string formats:
  PostgreSQL: postgresql://user:pass@host:5432/dbname
  MySQL:      mysql://user:pass@host:3306/dbname
  MongoDB:    mongodb://user:pass@host:27017/dbname`,
		Example: `  # PostgreSQL migration
  migrationdb --source postgresql://user:pass@source:5432/db --target postgresql://user:pass@target:5432/db

  # MySQL migration
  migrationdb --source mysql://user:pass@source:3306/db --target mysql://user:pass@target:3306/db

  # Stream to stdout
  migrationdb --source postgresql://user:pass@source:5432/db --stdout > dump.sql

  # Stream to stdout and compress
  migrationdb --source postgresql://user:pass@source:5432/db --stdout | gzip > dump.sql.gz

  # Skip verification
  migrationdb --source postgresql://user:pass@source:5432/db --target postgresql://user:pass@target:5432/db --skip-verify`,
		RunE: runMigration,
		// Silence usage on error
		SilenceUsage: true,
		// Silence error printing as we handle it in Execute()
		SilenceErrors: true,
	}
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Persistent flags for root command
	rootCmd.PersistentFlags().StringVar(&cfg.SourceConn, "source", "", "Source connection string")
	rootCmd.PersistentFlags().StringVar(&cfg.TargetConn, "target", "", "Target connection string")
	rootCmd.PersistentFlags().BoolVar(&cfg.StdoutMode, "stdout", false, "Stream to stdout instead of target database")
	rootCmd.PersistentFlags().StringVar(&cfg.LogLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().IntVar(&cfg.BufferSize, "buffer-size", 10*1024*1024, "Buffer size in bytes for streaming")
	rootCmd.PersistentFlags().DurationVar(&cfg.Timeout, "timeout", 24*time.Hour, "Migration timeout duration")

	// Add verification flags
	rootCmd.PersistentFlags().BoolVar(&cfg.SkipVerification, "skip-verify", false, "Skip verification after migration")
	rootCmd.PersistentFlags().IntVar(&cfg.VerifyChunkSize, "verify-chunk-size", 10*1024*1024, "Chunk size in bytes for verification streaming")
	rootCmd.PersistentFlags().BoolVar(&cfg.SkipTLSVerify, "skip-tls-verify", false, "Skip TLS certificate verification when testing connections")
	// allow passing custom dumper args (repeatable) for the selected database type
	rootCmd.PersistentFlags().StringArrayVar(&cfg.DumpArgs, "dump-arg", nil, "Additional dump argument(s) passed to the underlying dumper (pg_dump, mysqldump, mongodump). Repeat flag to add multiple, e.g., --dump-arg=--schema=public")
	// allow passing custom restorer args (repeatable) for the selected database type
	rootCmd.PersistentFlags().StringArrayVar(&cfg.RestoreArgs, "restore-arg", nil, "Additional restore argument(s) passed to the underlying restorer (pg_restore, mysql, mongorestore). Repeat flag to add multiple, e.g., --restore-arg=--schema=public")

	// Mark required flags
	_ = rootCmd.MarkPersistentFlagRequired("source")

	// Add sub-commands
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(validateCmd)
}

// inferDatabaseType determines the database type from a connection string
func inferDatabaseType(connString string) (string, error) {
	if connString == "" {
		return "", fmt.Errorf("connection string is empty")
	}

	// Handle MongoDB connection strings
	if strings.HasPrefix(connString, "mongodb://") || strings.HasPrefix(connString, "mongodb+srv://") {
		return "mongodb", nil
	}

	// Parse URL for other database types
	u, err := url.Parse(connString)
	if err != nil {
		return "", fmt.Errorf("invalid connection string: %v", err)
	}

	switch u.Scheme {
	case "postgres", "postgresql":
		return "postgres", nil
	case "mysql":
		return "mysql", nil
	default:
		return "", fmt.Errorf("unsupported database type in connection string: %s", u.Scheme)
	}
}

func setupSignalHandler(ctx context.Context, cancel context.CancelFunc, logger *config.Logger) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-signalChan:
			logger.Infof("Received signal: %v", sig)
			cancel()
		case <-ctx.Done():
			return
		}
	}()
}

func runMigration(cmd *cobra.Command, args []string) error {
	// Validate source and target connections
	dbType, err := ValidateConnections(cfg.SourceConn, cfg.TargetConn, cfg.StdoutMode)
	if err != nil {
		return err
	}

	// Update config with inferred types
	cfg.SourceType = dbType
	if !cfg.StdoutMode {
		cfg.TargetType = dbType
	}

	// Configure logger based on stdout mode
	loggerOpts := config.LoggerOptions{
		ForceStderr: cfg.StdoutMode, // Force stderr logging when using stdout mode
	}
	logger := config.NewLogger(cfg.LogLevel, loggerOpts)

	// Test source connection
	logger.Info("Testing source connection...")
	if err := TestConnection(dbType, cfg.SourceConn, cfg.SkipTLSVerify); err != nil {
		return fmt.Errorf("source connection (%s) test failed: %w", MaskConnectionString(cfg.SourceConn), err)
	}

	logger.Info("Source connection test successful!")

	// Test target connection
	logger.Info("Testing target connection...")
	if err := TestConnection(dbType, cfg.TargetConn, cfg.SkipTLSVerify); err != nil {
		return fmt.Errorf("target connection (%s) test failed: %w", MaskConnectionString(cfg.TargetConn), err)
	}

	// Log startup information to stderr
	logger.Infof("Starting migration - Database type: %s", dbType)
	if cfg.StdoutMode {
		logger.Info("Running in stdout mode - all logs will be written to stderr")
	}

	// Create migration manager
	manager := migration.NewManager(cfg, logger)

	// Setup context with cancellation
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	// Setup signal handling
	setupSignalHandler(ctx, cancel, logger)

	// Start migration
	if err := manager.Migrate(ctx); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("migration timed out after %v", cfg.Timeout)
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return fmt.Errorf("migration canceled by user")
		}
		return fmt.Errorf("migration failed: %w", err)
	}

	logger.Info("Migration completed successfully!")

	// Skip verification if requested or in stdout mode
	if cfg.SkipVerification || cfg.StdoutMode {
		if cfg.SkipVerification {
			logger.Info("Skipping verification as requested")
		}
		return nil
	}

	// Create verifier
	logger.Info("Starting verification...")

	sourceDumper, err := migration.CreateDumper(cfg.SourceType, cfg.SourceConn, cfg.StdoutMode, cfg.DumpArgs)
	targetDumper, err := migration.CreateDumper(cfg.TargetType, cfg.TargetConn, cfg.StdoutMode, cfg.DumpArgs)

	if err != nil {
		return fmt.Errorf("failed to create dumper: %w", err)
	}

	verifier, err := migration.NewDatabaseVerifier(
		sourceDumper,
		targetDumper,
		migration.WithChunkSize(cfg.VerifyChunkSize),
	)
	if err != nil {
		return fmt.Errorf("failed to create verifier: %w", err)
	}

	// Create a new context for verification
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer verifyCancel()

	// Setup signal handling for verification
	setupSignalHandler(verifyCtx, verifyCancel, logger)

	// Start verification
	if err := verifier.VerifyContent(verifyCtx); err != nil {
		if errors.Is(verifyCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("verification timed out after %v", cfg.Timeout)
		}
		if errors.Is(verifyCtx.Err(), context.Canceled) {
			return fmt.Errorf("verification canceled by user")
		}
		return fmt.Errorf("verification failed: %w", err)
	}

	// Calculate checksum
	checksum, err := verifier.GetChecksum(verifyCtx)
	if err != nil {
		logger.Warnf("Failed to calculate checksum: %v", err)
	} else {
		logger.Infof("Database checksum: %s", checksum)
	}

	logger.Info("Verification completed successfully!")
	return nil
}
