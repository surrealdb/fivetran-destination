package connector

import (
	"encoding/csv"
	"fmt"
	"io"
	"time"

	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

func (s *Server) processCSVRecords(files []string, fileParams *pb.FileParams, keys map[string][]byte, process func(columns []string, record []string) error) error {
	// Track file processing timing
	if s.metrics != nil {
		s.metrics.FileProcessingStarted()
		defer func(start time.Time) {
			s.metrics.FileProcessingCompleted(time.Since(start))
		}(time.Now())
	}

	for _, f := range files {
		r, err := s.openFivetranFile(f, fileParams, keys)
		if err != nil {
			return fmt.Errorf("failed to open fivetran file: %w", err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				s.logWarning("failed to close fivetran file", err)
			}
		}()

		cr := csv.NewReader(r)

		// TODO: ReuseRecord to avoid allocating a new slice for each record?

		columns, err := cr.Read()
		if err != nil {
			return fmt.Errorf("failed to read csv columns: %w", err)
		}

		// Track file processing
		if s.metrics != nil {
			s.metrics.FileProcessed()
		}

		var recordCount int64
		var bytesProcessed int64

		for {
			record, err := cr.Read()
			if err != nil && err != io.EOF {
				if s.metrics != nil {
					s.metrics.FileProcessingError()
				}
				return fmt.Errorf("failed to read csv record: %w", err)
			}
			if err == io.EOF {
				break
			}

			// Calculate approximate bytes for this record
			var recordBytes int64
			for _, field := range record {
				recordBytes += int64(len(field)) + 1 // +1 for delimiter
			}

			if err := process(columns, record); err != nil {
				if s.metrics != nil {
					s.metrics.FileProcessingError()
				}
				return fmt.Errorf("failed to process csv record: %w", err)
			}

			recordCount++
			bytesProcessed += recordBytes
		}

		// Update metrics after processing the file
		if s.metrics != nil && recordCount > 0 {
			s.metrics.RecordProcessed(recordCount, bytesProcessed)
		}
	}
	return nil
}

// Returns a decrypted and decompressed stream of the file content.
// The original file is compressed using zstd, and then encrypted.
// The encryption algorithm is specified in fileParams.Encryption.
// The key is specified in keys.
// In case of the CBC mode of AES, iv is prepended to the ciphertext within the file.
//
// It's the caller's responsibility to close the returned reader.
func (s *Server) openFivetranFile(file string, fileParams *pb.FileParams, keys map[string][]byte) (io.ReadCloser, error) {
	key, ok := keys[file]
	if !ok {
		return nil, fmt.Errorf("key not found for file: %s", file)
	}

	r, err := NewFivetranFileReader(file, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create fivetran file reader: %w", err)
	}

	return r, nil
}
