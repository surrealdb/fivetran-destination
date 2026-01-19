package testframework

import (
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
)

// NewWriteBatchRequest creates a WriteBatchRequest with the provided parameters
func NewWriteBatchRequest(
	config map[string]string,
	schemaName string,
	table *pb.Table,
	replaceFiles []string,
	updateFiles []string,
	deleteFiles []string,
	keys map[string][]byte,
	params *pb.FileParams,
) *pb.WriteBatchRequest {
	return &pb.WriteBatchRequest{
		Configuration: config,
		SchemaName:    schemaName,
		Table:         table,
		ReplaceFiles:  replaceFiles,
		UpdateFiles:   updateFiles,
		DeleteFiles:   deleteFiles,
		Keys:          keys,
		FileParams:    params,
	}
}

// GetTestFileParams returns standard FileParams for testing
// Uses Zstd compression and AES encryption with standard null/unmodified strings
func GetTestFileParams() *pb.FileParams {
	return &pb.FileParams{
		Compression:      pb.Compression_ZSTD,
		Encryption:       pb.Encryption_AES,
		NullString:       "nullstring01234",
		UnmodifiedString: "unmodifiedstring56789",
	}
}

// GetUnencryptedFileParams returns FileParams for unencrypted/uncompressed files
func GetUnencryptedFileParams() *pb.FileParams {
	return &pb.FileParams{
		Compression:      pb.Compression_OFF,
		Encryption:       pb.Encryption_NONE,
		NullString:       "nullstring01234",
		UnmodifiedString: "unmodifiedstring56789",
	}
}
