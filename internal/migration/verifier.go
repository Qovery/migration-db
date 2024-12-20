package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// Verifier defines the interface for database verification operations
type Verifier interface {
	VerifyContent(ctx context.Context) error
	GetChecksum(ctx context.Context) (string, error)
}

const defaultChunkSize = 10 * 1024 * 1024 // 10MB chunks by default

// DatabaseVerifier implements verification for any database type
type DatabaseVerifier struct {
	sourceDumper Dumper
	targetDumper Dumper
	chunkSize    int
}

// VerifierOption defines function type for verifier options
type VerifierOption func(*DatabaseVerifier)

// WithChunkSize sets a custom chunk size for comparison
func WithChunkSize(size int) VerifierOption {
	return func(v *DatabaseVerifier) {
		if size > 0 {
			v.chunkSize = size
		}
	}
}

// NewDatabaseVerifier creates a new verifier instance
func NewDatabaseVerifier(sourceDumper, targetDumper Dumper, opts ...VerifierOption) (*DatabaseVerifier, error) {
	if sourceDumper == nil || targetDumper == nil {
		return nil, fmt.Errorf("source and target dumpers must not be nil")
	}

	if sourceDumper.GetType() != targetDumper.GetType() {
		return nil, fmt.Errorf("source and target databases must be of the same type")
	}

	v := &DatabaseVerifier{
		sourceDumper: sourceDumper,
		targetDumper: targetDumper,
		chunkSize:    defaultChunkSize,
	}

	// Apply options
	for _, opt := range opts {
		opt(v)
	}

	return v, nil
}

// compareReaders compares two io.Reader streams chunk by chunk
func (v *DatabaseVerifier) compareReaders(ctx context.Context, source, target io.Reader) (bool, error) {
	sourceChunk := make([]byte, v.chunkSize)
	targetChunk := make([]byte, v.chunkSize)

	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
			// Read chunks from both sources
			sourceN, sourceErr := io.ReadFull(source, sourceChunk)
			targetN, targetErr := io.ReadFull(target, targetChunk)

			// Handle read results
			if sourceErr != nil && sourceErr != io.EOF && !errors.Is(sourceErr, io.ErrUnexpectedEOF) {
				return false, fmt.Errorf("error reading source: %w", sourceErr)
			}
			if targetErr != nil && targetErr != io.EOF && !errors.Is(targetErr, io.ErrUnexpectedEOF) {
				return false, fmt.Errorf("error reading target: %w", targetErr)
			}

			// Compare chunks based on database type
			switch v.sourceDumper.GetType() {
			case Postgres:
				if !bytes.Equal(sourceChunk[:sourceN], targetChunk[:targetN]) {
					return false, nil
				}
			case MySQL:
				normalizedSource, err := normalizeMySQLDump(sourceChunk[:sourceN])
				if err != nil {
					return false, err
				}
				normalizedTarget, err := normalizeMySQLDump(targetChunk[:targetN])
				if err != nil {
					return false, err
				}
				if !bytes.Equal(normalizedSource, normalizedTarget) {
					return false, nil
				}
			case MongoDB:
				normalizedSource, err := normalizeMongoDBDump(sourceChunk[:sourceN])
				if err != nil {
					return false, err
				}
				normalizedTarget, err := normalizeMongoDBDump(targetChunk[:targetN])
				if err != nil {
					return false, err
				}
				if !bytes.Equal(normalizedSource, normalizedTarget) {
					return false, nil
				}
			}

			// Check if we've reached the end of both streams
			if sourceErr == io.EOF || errors.Is(sourceErr, io.ErrUnexpectedEOF) {
				if targetErr == io.EOF || errors.Is(targetErr, io.ErrUnexpectedEOF) {
					// Both streams ended
					return sourceN == targetN, nil
				}
				// Source ended but target didn't
				return false, nil
			}
			if targetErr == io.EOF || errors.Is(targetErr, io.ErrUnexpectedEOF) {
				// Target ended but source didn't
				return false, nil
			}
		}
	}
}

// VerifyContent performs verification of the migration by comparing dumps
func (v *DatabaseVerifier) VerifyContent(ctx context.Context) error {
	// Create pipes for streaming the dumps
	sourceReader, sourceWriter := io.Pipe()
	targetReader, targetWriter := io.Pipe()

	// Create error channels for the dump operations
	sourceDumpErr := make(chan error, 1)
	targetDumpErr := make(chan error, 1)
	compareErr := make(chan error, 1)

	// Start dumping source database
	go func() {
		defer sourceWriter.Close()
		err := v.sourceDumper.Dump(ctx, sourceWriter)
		sourceDumpErr <- err
	}()

	// Start dumping target database
	go func() {
		defer targetWriter.Close()
		err := v.targetDumper.Dump(ctx, targetWriter)
		targetDumpErr <- err
	}()

	// Start comparison in a goroutine
	go func() {
		equal, err := v.compareReaders(ctx, sourceReader, targetReader)
		if err != nil {
			compareErr <- err
			return
		}
		if !equal {
			compareErr <- fmt.Errorf("content verification failed: source and target databases do not match")
			return
		}
		compareErr <- nil
	}()

	// Wait for all operations to complete or context to cancel
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-sourceDumpErr:
		if err != nil {
			return fmt.Errorf("failed to dump source database: %w", err)
		}
	case err := <-targetDumpErr:
		if err != nil {
			return fmt.Errorf("failed to dump target database: %w", err)
		}
	case err := <-compareErr:
		if err != nil {
			return err
		}
	}

	return nil
}

// GetChecksum generates a checksum of the database content using streaming
func (v *DatabaseVerifier) GetChecksum(ctx context.Context) (string, error) {
	reader, writer := io.Pipe()
	hashErr := make(chan error, 1)
	hash := sha256.New()

	// Start calculating hash in a goroutine
	go func() {
		defer reader.Close()
		_, err := io.Copy(hash, reader)
		hashErr <- err
	}()

	// Dump the database
	if err := v.targetDumper.Dump(ctx, writer); err != nil {
		writer.Close()
		return "", fmt.Errorf("failed to dump database for checksum: %w", err)
	}
	writer.Close()

	// Wait for hash calculation to complete
	if err := <-hashErr; err != nil {
		return "", fmt.Errorf("failed to calculate checksum: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// normalizeMySQLDump normalizes a MySQL dump chunk for comparison
func normalizeMySQLDump(chunk []byte) ([]byte, error) {
	// Remove timestamps, variable content, and other non-deterministic elements
	lines := bytes.Split(chunk, []byte("\n"))
	var normalized [][]byte

	for _, line := range lines {
		// Skip lines that contain non-deterministic content
		if bytes.HasPrefix(line, []byte("-- Dump completed on")) ||
			bytes.HasPrefix(line, []byte("-- MySQL dump")) ||
			bytes.HasPrefix(line, []byte("-- Server version")) {
			continue
		}
		normalized = append(normalized, line)
	}

	return bytes.Join(normalized, []byte("\n")), nil
}

// normalizeMongoDBDump normalizes a MongoDB dump chunk for comparison
func normalizeMongoDBDump(chunk []byte) ([]byte, error) {
	// MongoDB dumps might contain ObjectIDs and timestamps that need normalization
	lines := bytes.Split(chunk, []byte("\n"))
	var normalized [][]byte

	for _, line := range lines {
		// Skip or normalize lines containing non-deterministic content
		if bytes.Contains(line, []byte("\"$timestamp\"")) ||
			bytes.Contains(line, []byte("\"$date\"")) {
			continue
		}
		normalized = append(normalized, line)
	}

	return bytes.Join(normalized, []byte("\n")), nil
}
