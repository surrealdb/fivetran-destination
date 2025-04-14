package connector

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// Fivetran sends the connector AES-encrypted Zstd-compressed files.
// This test verifies that the file reading works as expected.
func TestFivetranFileReading(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	data := []byte("Hello, World!")

	tmpFile, compressed, _, ivWithData := createEncryptedZstdFile(t, key, data)

	log.Printf("compressed: %v", compressed)
	log.Printf("ivWithData: %v", ivWithData)
	reader, err := NewFivetranFileReader(tmpFile, key)
	if err != nil {
		t.Fatalf("failed to create fivetran file reader: %v", err)
	}

	read, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read decompressed data: %v", err)
	}

	require.NotEmpty(t, read)

	// strip padding
	// Find last non-zero byte to strip padding
	lastNonZero := len(read) - 1
	for ; lastNonZero >= 0 && read[lastNonZero] == 0; lastNonZero-- {
	}
	read = read[:lastNonZero+1]

	require.Equal(t, data, read)
}

func TestCreateEncryptedZstdFile(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	data := []byte("Hello, World!")

	tmpFile, compressed, padded, e := createEncryptedZstdFile(t, key, data)
	t.Cleanup(func() {
		err := os.Remove(tmpFile)
		if err != nil {
			t.Errorf("failed to remove temp file: %v", err)
		}
	})

	f, err := os.Open(tmpFile)
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}

	encrypted, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("failed to read temp file: %v", err)
	}

	require.NotEqual(t, data, encrypted)
	require.Equal(t, e, encrypted)

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("failed to create cipher: %v", err)
	}

	iv := make([]byte, aes.BlockSize)
	iv = encrypted[:aes.BlockSize]
	encrypted = encrypted[aes.BlockSize:]
	stream := cipher.NewCBCDecrypter(block, iv)

	decrypted := make([]byte, len(encrypted))
	stream.CryptBlocks(decrypted, encrypted)

	require.Equal(t, padded, decrypted)

	// Trim trailing zeros
	unpadded := bytes.TrimRight(decrypted, "\x00")

	log.Printf("decrypted: %v", decrypted)
	log.Printf("unpadded: %v", unpadded)

	zstdReader, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("failed to create zstd reader: %v", err)
	}

	decompressed, err := io.ReadAll(zstdReader)
	if err != nil {
		t.Fatalf("failed to read temp file: %v", err)
	}

	require.Equal(t, data, decompressed)
}

func createEncryptedZstdFile(t *testing.T, key []byte, data []byte) (string, []byte, []byte, []byte) {
	compressed := bytes.NewBuffer(nil)
	writer, err := zstd.NewWriter(compressed)
	require.NoError(t, err)

	_, err = writer.Write(data)
	require.NoError(t, err)

	require.NoError(t, writer.Close())

	block, err := aes.NewCipher(key)
	require.NoError(t, err)

	iv := make([]byte, aes.BlockSize)
	stream := cipher.NewCBCEncrypter(block, iv)

	// Pad input to block size since CBC requires full blocks
	padding := aes.BlockSize - (compressed.Len() % aes.BlockSize)
	padded := make([]byte, compressed.Len()+padding)
	copy(padded, compressed.Bytes())
	// The Fivetran padding? The last byte is the padding length.
	padded[len(padded)-1] = byte(padding)
	for i := len(compressed.Bytes()); i < len(padded)-1; i++ {
		padded[i] = byte(0)
	}
	encrypted := make([]byte, len(padded))
	stream.CryptBlocks(encrypted, padded)

	tmpDir := t.TempDir()
	tname := strings.ReplaceAll(t.Name(), "/", "_")
	tmpFile := filepath.Join(tmpDir, fmt.Sprintf("test.%s.zstd", tname))

	// The file content is `iv` followed by ciphertext.
	var ciphertext []byte
	ciphertext = append(ciphertext, iv...)
	ciphertext = append(ciphertext, encrypted...)

	if err := os.WriteFile(tmpFile, ciphertext, 0644); err != nil {
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			t.Errorf("failed to remove temp file: %v", err)
		}
	})

	return tmpFile, compressed.Bytes(), padded, ciphertext
}

func TestBlockModeDecryptingReadCloser(t *testing.T) {
	block, err := aes.NewCipher([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("failed to create cipher: %v", err)
	}

	iv := make([]byte, aes.BlockSize)
	stream := cipher.NewCBCEncrypter(block, iv)

	data := []byte("Hello, World!")
	padding := aes.BlockSize - len(data)%aes.BlockSize
	padded := make([]byte, len(data)+padding)
	copy(padded, data)
	padded[len(padded)-1] = byte(padding)
	for i := len(data); i < len(padded)-1; i++ {
		padded[i] = byte(0)
	}

	encrypted := make([]byte, len(padded))
	stream.CryptBlocks(encrypted, padded)

	if len(encrypted) != len(padded) {
		t.Fatalf("encrypted data length is not equal to data length")
	}
	require.NotEqual(t, padded, encrypted)

	readBlockMode := cipher.NewCBCDecrypter(block, iv)
	reader := NewBlockModeDecryptingReadCloser(
		readBlockMode,
		io.NopCloser(bytes.NewReader(encrypted)),
		int64(len(encrypted)),
		// 2MB for now
		2*1024*1024,
	)

	read, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read decompressed data: %v", err)
	}

	require.Equal(t, data, read)
}

func TestAESBlockMode(t *testing.T) {
	block, err := aes.NewCipher([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("failed to create cipher: %v", err)
	}

	iv := make([]byte, aes.BlockSize)
	stream := cipher.NewCBCEncrypter(block, iv)

	data := []byte("Hello, World!")
	padding := aes.BlockSize - len(data)%aes.BlockSize
	for i := 0; i < padding; i++ {
		data = append(data, byte(0))
	}

	encrypted := make([]byte, len(data))
	stream.CryptBlocks(encrypted, data)

	require.Equal(t, []byte{0x97, 0x51, 0x86, 0xdb, 0xe9, 0x66, 0xa3, 0xea, 0xb8, 0x34, 0xa7, 0xc2, 0x5, 0x68, 0x86, 0x2b}, encrypted)

	decryptor := cipher.NewCBCDecrypter(block, iv)

	decrypted := make([]byte, len(encrypted))
	decryptor.CryptBlocks(decrypted, encrypted)

	require.Equal(t, data, decrypted)
}

func TestZstdReadCloser(t *testing.T) {
	compressed := bytes.NewBuffer(nil)
	writer, err := zstd.NewWriter(compressed)
	if err != nil {
		t.Fatalf("failed to create zstd writer: %v", err)
	}

	writer.Write([]byte("Hello, World!"))
	writer.Close()

	reader := NewZstdReadCloser(io.NopCloser(compressed))

	read, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read zstd data: %v", err)
	}

	require.Equal(t, []byte("Hello, World!"), read)
}

func TestZstd(t *testing.T) {
	compressed := bytes.NewBuffer(nil)
	writer, err := zstd.NewWriter(compressed)
	if err != nil {
		t.Fatalf("failed to create zstd writer: %v", err)
	}

	writer.Write([]byte("Hello, World!"))
	writer.Close()

	reader, err := zstd.NewReader(compressed)
	if err != nil {
		t.Fatalf("failed to create zstd reader: %v", err)
	}

	read, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read zstd data: %v", err)
	}

	require.Equal(t, []byte("Hello, World!"), read)
}

func TestPaddedReadCloser(t *testing.T) {
	blockSize := 16

	data := []byte("Hello, World!")
	padding := blockSize - len(data)%blockSize
	padded := make([]byte, len(data)+padding)
	copy(padded, data)
	for i := 0; i < padding; i++ {
		padded[len(padded)-padding+i] = byte(0)
	}

	reader := NewPaddedReadCloser(&nonReadSeekCloser{bytes.NewReader(padded)}, blockSize)

	read, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read padded data: %v", err)
	}

	require.Equal(t, data, read)
}

type nonReadSeekCloser struct {
	*bytes.Reader
}

var _ io.ReadSeekCloser = &nonReadSeekCloser{}

func (c *nonReadSeekCloser) Close() error {
	return nil
}
