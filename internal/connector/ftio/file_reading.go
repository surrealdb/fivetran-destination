package ftio

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/klauspost/compress/zstd"
)

type FivetranFileReader struct {
	io.ReadCloser
}

func NewFivetranFileReader(file string, key []byte) (*FivetranFileReader, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	iv := make([]byte, aes.BlockSize)
	_, err = f.Read(iv)
	if err != nil {
		return nil, fmt.Errorf("failed to read iv: %w", err)
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	fileSize := fileInfo.Size() - int64(aes.BlockSize)

	blockMode := cipher.NewCBCDecrypter(block, iv)
	if err != nil {
		return nil, fmt.Errorf("failed to create aesgcm: %w", err)
	}

	// TODO For now, we use block size * megabytes as the read buffer size.
	decryptingReader := NewBlockModeDecryptingReadCloser(blockMode, f, fileSize, aes.BlockSize*1024*1024)

	decompressingReader := NewZstdReadCloser(decryptingReader)

	return &FivetranFileReader{
		ReadCloser: decompressingReader,
	}, nil
}

// BlockReadCloser is a reader that decrypts the input data in blocks.
// This is useful for reading data encrypted with AES in CBC mode.
type BlockReadCloser struct {
	readBuf []byte

	decryptionBuf []byte
	bufLen        int
	bufCursor     int

	block cipher.BlockMode
	in    io.ReadCloser

	fileSize         int64
	remainingFileLen int64
}

var _ io.ReadCloser = &BlockReadCloser{}

func NewBlockModeDecryptingReadCloser(blockMode cipher.BlockMode, in io.ReadCloser, fileSize int64, readBufferSize int) *BlockReadCloser {
	if readBufferSize%blockMode.BlockSize() != 0 {
		panic(fmt.Sprintf("readBufferSize %d is not a multiple of the block size %d", readBufferSize, blockMode.BlockSize()))
	}

	if fileSize%int64(blockMode.BlockSize()) != 0 {
		panic(fmt.Sprintf("fileSize %d is not a multiple of the block size %d", fileSize, blockMode.BlockSize()))
	}

	return &BlockReadCloser{
		readBuf: make([]byte, readBufferSize),
		// Otherwise we get `crypto/cipher: output smaller than input`
		decryptionBuf:    make([]byte, readBufferSize),
		block:            blockMode,
		in:               in,
		fileSize:         fileSize,
		remainingFileLen: fileSize,
	}
}

func (b *BlockReadCloser) Read(p []byte) (n int, err error) {
	var read int
	readCount := 0
	maxReadCount := 2

	maxBytesToRead := len(p)

	if b.debugging() {
		log.Printf("Decrypting reader reading up to %d bytes to the provided buffer", maxBytesToRead)
	}
	for read < maxBytesToRead {
		if b.bufCursor >= b.bufLen {
			err := b.readOn()
			if read > 0 && errors.Is(err, io.EOF) {
				return read, nil
			}

			if err != nil {
				return 0, err
			}
			b.bufCursor = 0
		}

		nextRead := maxBytesToRead - read
		if nextRead > b.bufLen-b.bufCursor {
			nextRead = b.bufLen - b.bufCursor
		}

		if b.debugging() {
			log.Printf("Reading %d bytes from the decryption buffer (cursor: %d, len: %d)", nextRead, b.bufCursor, b.bufLen)
		}

		copy(p[read:], b.decryptionBuf[b.bufCursor:b.bufCursor+nextRead])

		if b.debugging() {
			log.Printf("Wrote %v to %d:%d on destination buffer", b.decryptionBuf[b.bufCursor:b.bufCursor+nextRead], read, read+nextRead)
		}
		b.bufCursor += nextRead
		read += nextRead

		readCount++
		if readCount >= maxReadCount {
			panic("readCount >= maxReadCount")
		}
	}

	return read, nil
}

// readOn reads the next block from the input reader and decrypts it.
func (b *BlockReadCloser) readOn() error {
	read, err := b.in.Read(b.readBuf)

	if b.debugging() {
		log.Printf("readOn read %d bytes. %d bytes remaining. Current error: %v", read, b.remainingFileLen-int64(read), err)
	}

	if read < b.block.BlockSize() && err == nil {
		return fmt.Errorf("read less than block size: %d", read)
	}

	if read%b.block.BlockSize() != 0 {
		return fmt.Errorf("read is not a multiple of the block size: %d", read)
	}

	if read > 0 {
		if b.debugging() {
			log.Printf("Read %d bytes of %v", read, b.readBuf[:read])
		}

		b.block.CryptBlocks(b.decryptionBuf, b.readBuf)

		b.remainingFileLen -= int64(read)

		if b.remainingFileLen == 0 {
			paddingLen := b.decryptionBuf[read-1]
			b.decryptionBuf = b.decryptionBuf[:read-int(paddingLen)]
			b.bufLen = read - int(paddingLen)

			// NOTE: Apparently the below is not how Fivetran do padding.
			//
			// // Trim trailing zeros added to make the data a multiple of the block size.
			// // Otherwise, the redundant zeroes might be read by the reader and
			// // cause errors, like downstream zstd decompression errors due to magic number mismatch.
			// b.decryptionBuf = bytes.TrimRight(b.decryptionBuf[:read], "\x00")
			// b.bufLen = len(b.decryptionBuf)
		} else {
			b.bufLen = read
		}

		if b.debugging() {
			log.Printf("Decrypted %d bytes of %v after unpadding", b.bufLen, b.decryptionBuf[:b.bufLen])
		}

		return nil
	}

	return err
}

func (b *BlockReadCloser) Close() error {
	return b.in.Close()
}

func (b *BlockReadCloser) debugging() bool {
	return os.Getenv("SURREAL_FIVETRAN_DEBUG") != ""
}

// ZstdReadCloser is a reader that decompresses the input data using Zstd.
type ZstdReadCloser struct {
	in             io.ReadCloser
	zstdReadCloser io.ReadCloser
}

var _ io.ReadCloser = &ZstdReadCloser{}

func NewZstdReadCloser(in io.ReadCloser) *ZstdReadCloser {
	return &ZstdReadCloser{in: in}
}

func (z *ZstdReadCloser) Read(p []byte) (int, error) {
	if z.zstdReadCloser == nil {
		reader, err := zstd.NewReader(z.in)
		if err != nil {
			return 0, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		z.zstdReadCloser = reader.IOReadCloser()
	}

	return z.zstdReadCloser.Read(p)

	// n, err := z.zstdReadCloser.Read(p)
	// if n > 0 {
	// 	return n, nil
	// }

	// if err != nil && !errors.Is(err, io.EOF) {
	// 	return 0, fmt.Errorf("failed to read zstd reader: %w", err)
	// }

	// return 0, err
}

func (z *ZstdReadCloser) Close() error {
	return z.zstdReadCloser.Close()
}

// paddedReadCloser is a reader that trims trailing zeroes from the last block.
type paddedReadCloser struct {
	in io.ReadSeekCloser

	blockSize int

	initialized bool

	sourceLen int64

	read int64
}

var _ io.ReadCloser = &paddedReadCloser{}

func NewPaddedReadCloser(in io.ReadSeekCloser, blockSize int) *paddedReadCloser {
	return &paddedReadCloser{in: in, blockSize: blockSize}
}

func (c *paddedReadCloser) Read(p []byte) (int, error) {
	buf := make([]byte, c.blockSize)

	if !c.initialized {
		fileLen, err := c.in.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, fmt.Errorf("failed to get file length: %w", err)
		}

		if fileLen%int64(c.blockSize) != 0 {
			return 0, fmt.Errorf("file length is not a multiple of the block size %d: %d", c.blockSize, fileLen)
		}

		// Read last block and ensure the padding is not included in the file length
		_, err = c.in.Seek(-int64(c.blockSize), io.SeekEnd)
		if err != nil {
			return 0, fmt.Errorf("failed to seek to the last block: %w", err)
		}

		read, err := c.in.Read(buf)
		if err != nil {
			return 0, fmt.Errorf("failed to read last block: %w", err)
		}

		log.Printf("read %d bytes as the last block", read)

		trimmed := bytes.TrimRight(buf, "\x00")

		log.Printf("last block size after trimming is %d", len(trimmed))

		numBlocks := fileLen / int64(c.blockSize)
		c.sourceLen = (numBlocks-1)*int64(c.blockSize) + int64(len(trimmed))

		_, err = c.in.Seek(0, io.SeekStart)
		if err != nil {
			return 0, fmt.Errorf("failed to seek to the start of the file: %w", err)
		}

		c.initialized = true
	}

	read, err := c.in.Read(p)

	if read != 0 {
		r := c.read + int64(read)

		if r > c.sourceLen {
			r = c.sourceLen
		}

		read = int(r - c.read)
		if read <= 0 {
			return 0, io.EOF
		}

		return read, nil
	}

	return read, err
}

func (p *paddedReadCloser) Close() error {
	return p.in.Close()
}
