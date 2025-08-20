package migration

import (
	"context"
	"data-migration/internal/config"
	"fmt"
	"io"
	"os"
)

type Manager struct {
	config *config.Config
	logger *config.Logger
}

func NewManager(cfg *config.Config, logger *config.Logger) *Manager {
	return &Manager{
		config: cfg,
		logger: logger,
	}
}

func (m *Manager) Migrate(ctx context.Context) error {
	// Create source dumper
	dumper, err := CreateDumper(m.config.SourceType, m.config.SourceConn, m.config.StdoutMode, m.config.DumpArgs)
	if err != nil {
		return fmt.Errorf("failed to create dumper: %w", err)
	}

	// Handle stdout mode
	if m.config.StdoutMode {
		m.logger.Info("Streaming database dump to stdout...")
		return dumper.Dump(ctx, os.Stdout)
	}

	// Create target restorer
	restorer, err := CreateRestorer(m.config.TargetType, m.config.TargetConn, m.config.RestoreArgs)
	if err != nil {
		return fmt.Errorf("failed to create restorer: %w", err)
	}

	// Create pipe for streaming
	reader, writer := io.Pipe()

	// Setup error channels
	errChan := make(chan error, 2)

	// Start dump process
	go func() {
		defer writer.Close()
		m.logger.Info("Starting database dump...")
		if err := dumper.Dump(ctx, writer); err != nil {
			m.logger.Errorf("Dump error: %v", err)
			errChan <- fmt.Errorf("dump failed: %w", err)
		} else {
			errChan <- nil
			m.logger.Info("Database dump completed")
		}
	}()

	// Start restore process
	go func() {
		m.logger.Info("Starting database restore...")
		if err := restorer.Restore(ctx, reader); err != nil {
			m.logger.Errorf("Restore error: %v", err)
			errChan <- fmt.Errorf("restore failed: %w", err)
		} else {
			errChan <- nil
			m.logger.Info("Database restore completed")
		}
	}()

	// Wait for both processes to complete
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}
