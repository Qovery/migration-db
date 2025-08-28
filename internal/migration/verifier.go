package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
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
	fmt.Fprintf(os.Stderr, "DEBUG: compareReaders started\n")

	sourceChunk := make([]byte, v.chunkSize)
	targetChunk := make([]byte, v.chunkSize)

	chunkCount := 0
	totalSourceBytes := 0
	totalTargetBytes := 0

	for {
		chunkCount++
		fmt.Fprintf(os.Stderr, "DEBUG: Starting chunk #%d\n", chunkCount)

		select {
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "DEBUG: Context cancelled in compareReaders\n")
			return false, ctx.Err()
		default:
			// Read chunks from both sources
			fmt.Fprintf(os.Stderr, "DEBUG: About to read source chunk #%d\n", chunkCount)
			sourceN, sourceErr := io.ReadFull(source, sourceChunk)
			fmt.Fprintf(os.Stderr, "DEBUG: Source chunk #%d read: %d bytes, err: %v\n", chunkCount, sourceN, sourceErr)
			totalSourceBytes += sourceN

			fmt.Fprintf(os.Stderr, "DEBUG: About to read target chunk #%d\n", chunkCount)
			targetN, targetErr := io.ReadFull(target, targetChunk)
			fmt.Fprintf(os.Stderr, "DEBUG: Target chunk #%d read: %d bytes, err: %v\n", chunkCount, targetN, targetErr)
			totalTargetBytes += targetN

			fmt.Fprintf(os.Stderr, "DEBUG: Total bytes so far - Source: %d, Target: %d\n", totalSourceBytes, totalTargetBytes)

			// Handle read results
			if sourceErr != nil && sourceErr != io.EOF && !errors.Is(sourceErr, io.ErrUnexpectedEOF) {
				fmt.Fprintf(os.Stderr, "DEBUG: Source read error: %v\n", sourceErr)
				return false, fmt.Errorf("error reading source: %w", sourceErr)
			}
			if targetErr != nil && targetErr != io.EOF && !errors.Is(targetErr, io.ErrUnexpectedEOF) {
				fmt.Fprintf(os.Stderr, "DEBUG: Target read error: %v\n", targetErr)
				return false, fmt.Errorf("error reading target: %w", targetErr)
			}

			fmt.Fprintf(os.Stderr, "DEBUG: About to compare chunks for database type: %s\n", v.sourceDumper.GetType())

			// Compare chunks based on database type
			switch v.sourceDumper.GetType() {
			case Postgres:
				fmt.Fprintf(os.Stderr, "DEBUG: Normalizing PostgreSQL chunks\n")
				normalizedSource, err := normalizePostgresDump(sourceChunk[:sourceN])
				if err != nil {
					fmt.Fprintf(os.Stderr, "DEBUG: Error normalizing source: %v\n", err)
					return false, fmt.Errorf("error normalizing source PostgreSQL dump: %w", err)
				}
				normalizedTarget, err := normalizePostgresDump(targetChunk[:targetN])
				if err != nil {
					fmt.Fprintf(os.Stderr, "DEBUG: Error normalizing target: %v\n", err)
					return false, fmt.Errorf("error normalizing target PostgreSQL dump: %w", err)
				}
				fmt.Fprintf(os.Stderr, "DEBUG: Comparing normalized chunks (source: %d bytes, target: %d bytes)\n", len(normalizedSource), len(normalizedTarget))
				if !bytes.Equal(normalizedSource, normalizedTarget) {
					fmt.Fprintf(os.Stderr, "DEBUG: Postgres chunks don't match (source: %d bytes, target: %d bytes)\n", sourceN, targetN)
					return false, nil
				}
				fmt.Fprintf(os.Stderr, "DEBUG: Postgres chunks match\n")
			case MySQL:
				fmt.Fprintf(os.Stderr, "DEBUG: Normalizing MySQL chunks\n")
				normalizedSource, err := normalizeMySQLDump(sourceChunk[:sourceN])
				if err != nil {
					return false, err
				}
				normalizedTarget, err := normalizeMySQLDump(targetChunk[:targetN])
				if err != nil {
					return false, err
				}
				if !bytes.Equal(normalizedSource, normalizedTarget) {
					fmt.Fprintf(os.Stderr, "DEBUG: MySQL chunks don't match\n")
					return false, nil
				}
				fmt.Fprintf(os.Stderr, "DEBUG: MySQL chunks match\n")
			case MongoDB:
				fmt.Fprintf(os.Stderr, "DEBUG: Normalizing MongoDB chunks\n")
				normalizedSource, err := normalizeMongoDBDump(sourceChunk[:sourceN])
				if err != nil {
					return false, err
				}
				normalizedTarget, err := normalizeMongoDBDump(targetChunk[:targetN])
				if err != nil {
					return false, err
				}
				if !bytes.Equal(normalizedSource, normalizedTarget) {
					fmt.Fprintf(os.Stderr, "DEBUG: MongoDB chunks don't match\n")
					return false, nil
				}
				fmt.Fprintf(os.Stderr, "DEBUG: MongoDB chunks match\n")
			}

			// Check if we've reached the end of both streams
			if sourceErr == io.EOF || errors.Is(sourceErr, io.ErrUnexpectedEOF) {
				fmt.Fprintf(os.Stderr, "DEBUG: Source stream ended\n")
				if targetErr == io.EOF || errors.Is(targetErr, io.ErrUnexpectedEOF) {
					fmt.Fprintf(os.Stderr, "DEBUG: Target stream ended, comparing final bytes: sourceN=%d, targetN=%d\n", sourceN, targetN)
					// Both streams ended
					result := sourceN == targetN
					fmt.Fprintf(os.Stderr, "DEBUG: Final comparison result: %v\n", result)
					return result, nil
				}
				// Source ended but target didn't
				fmt.Fprintf(os.Stderr, "DEBUG: Source ended but target didn't - mismatch\n")
				return false, nil
			}
			if targetErr == io.EOF || errors.Is(targetErr, io.ErrUnexpectedEOF) {
				// Target ended but source didn't
				fmt.Fprintf(os.Stderr, "DEBUG: Target ended but source didn't - mismatch\n")
				return false, nil
			}

			fmt.Fprintf(os.Stderr, "DEBUG: Chunk #%d processed successfully, continuing to next chunk\n", chunkCount)
		}
	}
}

// VerifyContent performs verification of the migration by comparing dumps
func (v *DatabaseVerifier) VerifyContent(ctx context.Context) error {
	fmt.Fprintf(os.Stderr, "DEBUG: VerifyContent started\n")

	// Create pipes for streaming the dumps
	sourceReader, sourceWriter := io.Pipe()
	targetReader, targetWriter := io.Pipe()
	fmt.Fprintf(os.Stderr, "DEBUG: Pipes created\n")

	// Create error channels for the dump operations
	sourceDumpErr := make(chan error, 1)
	targetDumpErr := make(chan error, 1)
	compareErr := make(chan error, 1)
	fmt.Fprintf(os.Stderr, "DEBUG: Error channels created\n")

	// Start dumping source database
	go func() {
		fmt.Fprintf(os.Stderr, "DEBUG: Source dump goroutine started\n")
		defer func() {
			fmt.Fprintf(os.Stderr, "DEBUG: Source dump goroutine closing writer\n")
			sourceWriter.Close()
		}()
		fmt.Fprintf(os.Stderr, "DEBUG: About to call source dumper.Dump()\n")
		err := v.sourceDumper.Dump(ctx, sourceWriter)
		fmt.Fprintf(os.Stderr, "DEBUG: Source dumper.Dump() completed with error: %v\n", err)
		sourceDumpErr <- err
		fmt.Fprintf(os.Stderr, "DEBUG: Source dump error sent to channel\n")
	}()

	// Start dumping target database
	go func() {
		fmt.Fprintf(os.Stderr, "DEBUG: Target dump goroutine started\n")
		defer func() {
			fmt.Fprintf(os.Stderr, "DEBUG: Target dump goroutine closing writer\n")
			targetWriter.Close()
		}()
		fmt.Fprintf(os.Stderr, "DEBUG: About to call target dumper.Dump()\n")
		err := v.targetDumper.Dump(ctx, targetWriter)
		fmt.Fprintf(os.Stderr, "DEBUG: Target dumper.Dump() completed with error: %v\n", err)
		targetDumpErr <- err
		fmt.Fprintf(os.Stderr, "DEBUG: Target dump error sent to channel\n")
	}()

	// Start comparison in a goroutine
	go func() {
		fmt.Fprintf(os.Stderr, "DEBUG: Comparison goroutine started\n")
		equal, err := v.compareReaders(ctx, sourceReader, targetReader)
		fmt.Fprintf(os.Stderr, "DEBUG: compareReaders completed: equal=%v, err=%v\n", equal, err)
		if err != nil {
			compareErr <- err
			fmt.Fprintf(os.Stderr, "DEBUG: Comparison error sent to channel: %v\n", err)
			return
		}
		if !equal {
			compareErr <- fmt.Errorf("content verification failed: source and target databases do not match")
			fmt.Fprintf(os.Stderr, "DEBUG: Databases don't match error sent to channel\n")
			return
		}
		compareErr <- nil
		fmt.Fprintf(os.Stderr, "DEBUG: Comparison success sent to channel\n")
	}()

	// Wait for all operations to complete - corrected logic
	var sourceDumpError, targetDumpError, compareError error
	completedOperations := 0
	fmt.Fprintf(os.Stderr, "DEBUG: Starting to wait for operations to complete\n")

	for completedOperations < 3 {
		fmt.Fprintf(os.Stderr, "DEBUG: Waiting for operation %d/3\n", completedOperations+1)
		select {
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "DEBUG: Context cancelled during wait\n")
			return ctx.Err()
		case err := <-sourceDumpErr:
			fmt.Fprintf(os.Stderr, "DEBUG: Received source dump result: %v\n", err)
			sourceDumpError = err
			completedOperations++
		case err := <-targetDumpErr:
			fmt.Fprintf(os.Stderr, "DEBUG: Received target dump result: %v\n", err)
			targetDumpError = err
			completedOperations++
		case err := <-compareErr:
			fmt.Fprintf(os.Stderr, "DEBUG: Received comparison result: %v\n", err)
			compareError = err
			completedOperations++
		}
	}

	fmt.Fprintf(os.Stderr, "DEBUG: All operations completed, checking errors\n")

	// Check for errors in order of importance
	if sourceDumpError != nil {
		fmt.Fprintf(os.Stderr, "DEBUG: Returning source dump error\n")
		return fmt.Errorf("failed to dump source database: %w", sourceDumpError)
	}
	if targetDumpError != nil {
		fmt.Fprintf(os.Stderr, "DEBUG: Returning target dump error\n")
		return fmt.Errorf("failed to dump target database: %w", targetDumpError)
	}
	if compareError != nil {
		fmt.Fprintf(os.Stderr, "DEBUG: Returning comparison error\n")
		return compareError
	}

	fmt.Fprintf(os.Stderr, "DEBUG: VerifyContent completed successfully\n")
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

// normalizePostgresDump normalizes a PostgreSQL dump chunk for comparison
func normalizePostgresDump(chunk []byte) ([]byte, error) {
	lines := bytes.Split(chunk, []byte("\n"))
	var normalized [][]byte

	for _, line := range lines {
		// Skip lines with timestamps and version info - using simple prefix matching
		if bytes.HasPrefix(line, []byte("-- Dumped on")) ||
			bytes.HasPrefix(line, []byte("-- Dumped by")) ||
			bytes.HasPrefix(line, []byte("-- Started on")) ||
			bytes.HasPrefix(line, []byte("-- Completed on")) ||
			bytes.HasPrefix(line, []byte("-- PostgreSQL database dump")) ||
			bytes.HasPrefix(line, []byte("-- pg_dump version")) ||
			bytes.HasPrefix(line, []byte("-- Server version")) ||
			bytes.HasPrefix(line, []byte("-- Name:")) ||
			bytes.HasPrefix(line, []byte("-- Type:")) ||
			bytes.HasPrefix(line, []byte("-- Schema:")) {
			continue
		}

		// Skip empty comment lines
		trimmed := bytes.TrimSpace(line)
		if bytes.Equal(trimmed, []byte("--")) || len(trimmed) == 0 {
			continue
		}

		// Keep the line as-is to avoid over-normalization
		normalized = append(normalized, line)
	}

	return bytes.Join(normalized, []byte("\n")), nil
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
