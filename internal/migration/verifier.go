package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"time"
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
	log.Printf("DEBUG: Starting comparison of readers")
	sourceChunk := make([]byte, v.chunkSize)
	targetChunk := make([]byte, v.chunkSize)

	totalSourceBytes := 0
	totalTargetBytes := 0
	chunkCount := 0

	for {
		select {
		case <-ctx.Done():
			log.Printf("DEBUG: Comparison cancelled due to context")
			return false, ctx.Err()
		default:
			chunkCount++
			log.Printf("DEBUG: Reading chunk #%d", chunkCount)

			// Read chunks from both sources
			sourceN, sourceErr := io.ReadFull(source, sourceChunk)
			targetN, targetErr := io.ReadFull(target, targetChunk)

			totalSourceBytes += sourceN
			totalTargetBytes += targetN

			log.Printf("DEBUG: Chunk #%d - Source read: %d bytes (err: %v), Target read: %d bytes (err: %v)",
				chunkCount, sourceN, sourceErr, targetN, targetErr)
			log.Printf("DEBUG: Total bytes so far - Source: %d, Target: %d", totalSourceBytes, totalTargetBytes)

			// Handle read results
			if sourceErr != nil && sourceErr != io.EOF && !errors.Is(sourceErr, io.ErrUnexpectedEOF) {
				log.Printf("DEBUG: Error reading source: %v", sourceErr)
				return false, fmt.Errorf("error reading source: %w", sourceErr)
			}
			if targetErr != nil && targetErr != io.EOF && !errors.Is(targetErr, io.ErrUnexpectedEOF) {
				log.Printf("DEBUG: Error reading target: %v", targetErr)
				return false, fmt.Errorf("error reading target: %w", targetErr)
			}

			// Compare chunks based on database type
			switch v.sourceDumper.GetType() {
			case Postgres:
				if !bytes.Equal(sourceChunk[:sourceN], targetChunk[:targetN]) {
					log.Printf("DEBUG: Postgres chunks don't match (source: %d bytes, target: %d bytes)", sourceN, targetN)
					return false, nil
				}
			case MySQL:
				normalizedSource, err := normalizeMySQLDump(sourceChunk[:sourceN])
				if err != nil {
					log.Printf("DEBUG: Error normalizing MySQL source: %v", err)
					return false, err
				}
				normalizedTarget, err := normalizeMySQLDump(targetChunk[:targetN])
				if err != nil {
					log.Printf("DEBUG: Error normalizing MySQL target: %v", err)
					return false, err
				}
				if !bytes.Equal(normalizedSource, normalizedTarget) {
					log.Printf("DEBUG: MySQL normalized chunks don't match")
					return false, nil
				}
			case MongoDB:
				normalizedSource, err := normalizeMongoDBDump(sourceChunk[:sourceN])
				if err != nil {
					log.Printf("DEBUG: Error normalizing MongoDB source: %v", err)
					return false, err
				}
				normalizedTarget, err := normalizeMongoDBDump(targetChunk[:targetN])
				if err != nil {
					log.Printf("DEBUG: Error normalizing MongoDB target: %v", err)
					return false, err
				}
				if !bytes.Equal(normalizedSource, normalizedTarget) {
					log.Printf("DEBUG: MongoDB normalized chunks don't match")
					return false, nil
				}
			}

			// Check if we've reached the end of both streams
			if sourceErr == io.EOF || errors.Is(sourceErr, io.ErrUnexpectedEOF) {
				if targetErr == io.EOF || errors.Is(targetErr, io.ErrUnexpectedEOF) {
					// Both streams ended
					log.Printf("DEBUG: Both streams ended, final comparison result: %v", sourceN == targetN)
					return sourceN == targetN, nil
				}
				// Source ended but target didn't
				log.Printf("DEBUG: Source ended but target didn't")
				return false, nil
			}
			if targetErr == io.EOF || errors.Is(targetErr, io.ErrUnexpectedEOF) {
				// Target ended but source didn't
				log.Printf("DEBUG: Target ended but source didn't")
				return false, nil
			}
		}
	}
}

// VerifyContent performs verification of the migration by comparing dumps
func (v *DatabaseVerifier) VerifyContent(ctx context.Context) error {
	start := time.Now()
	log.Printf("DEBUG: VerifyContent started at %v", start)

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
		log.Printf("DEBUG: Source dump goroutine started at %v", time.Since(start))
		err := v.sourceDumper.Dump(ctx, sourceWriter)
		log.Printf("DEBUG: Source dump completed at %v with error: %v", time.Since(start), err)
		sourceDumpErr <- err
	}()

	// Start dumping target database
	go func() {
		defer targetWriter.Close()
		log.Printf("DEBUG: Target dump goroutine started at %v", time.Since(start))
		err := v.targetDumper.Dump(ctx, targetWriter)
		log.Printf("DEBUG: Target dump completed at %v with error: %v", time.Since(start), err)
		targetDumpErr <- err
	}()

	// Start comparison in a goroutine
	go func() {
		log.Printf("DEBUG: Comparison goroutine started at %v", time.Since(start))
		log.Printf("DEBUG: About to compare readers - sourceReader: %T, targetReader: %T", sourceReader, targetReader)

		// Check if readers are still open/valid
		log.Printf("DEBUG: sourceReader state: %+v", sourceReader)
		log.Printf("DEBUG: targetReader state: %+v", targetReader)

		equal, err := v.compareReaders(ctx, sourceReader, targetReader)
		if err != nil {
			log.Printf("DEBUG: Comparison failed at %v with error: %v", time.Since(start), err)
			compareErr <- err
			return
		}
		if !equal {
			log.Printf("DEBUG: Comparison completed at %v - databases do NOT match", time.Since(start))
			compareErr <- fmt.Errorf("content verification failed: source and target databases do not match")
			return
		}
		log.Printf("DEBUG: Comparison completed at %v - databases match", time.Since(start))
		compareErr <- nil
	}()

	// Wait for all operations to complete or context to cancel
	log.Printf("DEBUG: Entering select statement at %v", time.Since(start))
	select {
	case <-ctx.Done():
		log.Printf("DEBUG: Context cancelled at %v", time.Since(start))
		return ctx.Err()
	case err := <-sourceDumpErr:
		log.Printf("DEBUG: Received from sourceDumpErr at %v: %v", time.Since(start), err)
		if err != nil {
			return fmt.Errorf("failed to dump source database: %w", err)
		}
		log.Printf("DEBUG: VerifyContent returning SUCCESS due to source dump completion at %v", time.Since(start))
	case err := <-targetDumpErr:
		log.Printf("DEBUG: Received from targetDumpErr at %v: %v", time.Since(start), err)
		if err != nil {
			return fmt.Errorf("failed to dump target database: %w", err)
		}
		log.Printf("DEBUG: VerifyContent returning SUCCESS due to target dump completion at %v", time.Since(start))
	case err := <-compareErr:
		log.Printf("DEBUG: Received from compareErr at %v: %v", time.Since(start), err)
		if err != nil {
			return err
		}
		log.Printf("DEBUG: VerifyContent returning SUCCESS due to comparison completion at %v", time.Since(start))
	}

	log.Printf("DEBUG: VerifyContent returning nil (success) at %v", time.Since(start))
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
