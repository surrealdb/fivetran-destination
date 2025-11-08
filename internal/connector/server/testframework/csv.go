package testframework

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// GenerateAESKey generates a random 32-byte AES-256 key
func GenerateAESKey() ([]byte, error) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random key: %w", err)
	}
	return key, nil
}

// WriteCSVContent creates CSV content from columns and records
func WriteCSVContent(columns []string, records [][]string) ([]byte, error) {
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)

	// Write header
	if err := writer.Write(columns); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write records
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("CSV writer error: %w", err)
	}

	return buf.Bytes(), nil
}

// CreateEncryptedCSV creates an AES-encrypted, Zstd-compressed CSV file
// Following Fivetran's file format: IV (16 bytes) + AES-CBC encrypted (Zstd compressed CSV)
func CreateEncryptedCSV(t *testing.T, tempDir, filename string, columns []string, records [][]string, key []byte) string {
	// Generate CSV content
	csvData, err := WriteCSVContent(columns, records)
	require.NoError(t, err, "Failed to write CSV content")

	// Compress with Zstd
	compressed := bytes.NewBuffer(nil)
	writer, err := zstd.NewWriter(compressed)
	require.NoError(t, err, "Failed to create zstd writer")

	_, err = writer.Write(csvData)
	require.NoError(t, err, "Failed to write data to zstd writer")

	require.NoError(t, writer.Close(), "Failed to close zstd writer")

	// Encrypt with AES-CBC
	block, err := aes.NewCipher(key)
	require.NoError(t, err, "Failed to create AES cipher")

	// Generate random IV
	iv := make([]byte, aes.BlockSize)
	_, err = rand.Read(iv)
	require.NoError(t, err, "Failed to generate IV")

	stream := cipher.NewCBCEncrypter(block, iv)

	// Pad to block size (PKCS#7-style padding)
	// The last byte contains the padding length
	padding := aes.BlockSize - (compressed.Len() % aes.BlockSize)
	padded := make([]byte, compressed.Len()+padding)
	copy(padded, compressed.Bytes())
	// Set padding bytes to 0, last byte is padding length
	padded[len(padded)-1] = byte(padding)
	for i := len(compressed.Bytes()); i < len(padded)-1; i++ {
		padded[i] = byte(0)
	}

	encrypted := make([]byte, len(padded))
	stream.CryptBlocks(encrypted, padded)

	// Write file: IV + encrypted data
	var fileContent []byte
	fileContent = append(fileContent, iv...)
	fileContent = append(fileContent, encrypted...)

	filePath := filepath.Join(tempDir, filename)
	err = os.WriteFile(filePath, fileContent, 0644)
	require.NoError(t, err, "Failed to write encrypted CSV file")

	return filePath
}

// CreateUnencryptedCSV creates a plain CSV file (for testing error cases)
func CreateUnencryptedCSV(t *testing.T, tempDir, filename string, columns []string, records [][]string) string {
	csvData, err := WriteCSVContent(columns, records)
	require.NoError(t, err, "Failed to write CSV content")

	filePath := filepath.Join(tempDir, filename)
	err = os.WriteFile(filePath, csvData, 0644)
	require.NoError(t, err, "Failed to write unencrypted CSV file")

	return filePath
}
