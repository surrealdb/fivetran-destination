package connector

import (
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/surrealdb/fivetran-destination/internal/connector/server"
	pb "github.com/surrealdb/fivetran-destination/internal/pb"
	"github.com/surrealdb/surrealdb.go"
)

func TestDescribeTable_all_data_types(t *testing.T) {
	surrealdbEndpoint := os.Getenv("SURREALDB_ENDPOINT")
	if surrealdbEndpoint == "" {
		t.Skip("SURREALDB_ENDPOINT is not set")
	}

	sdb, err := surrealdb.FromEndpointURLString(t.Context(), surrealdbEndpoint)
	if err != nil {
		t.Fatalf("failed to connect to surrealdb: %v", err)
	}

	_, err = sdb.SignIn(t.Context(), &surrealdb.Auth{
		Username: "root",
		Password: "root",
	})
	require.NoError(t, err)

	err = sdb.Use(t.Context(), "test", "test")
	require.NoError(t, err)

	_, err = surrealdb.Query[any](t.Context(), sdb, "REMOVE TABLE IF EXISTS txn1;", nil)
	require.NoError(t, err)

	srv := server.New(zerolog.New(os.Stdout).Level(zerolog.DebugLevel))
	created, err := srv.CreateTable(t.Context(), &pb.CreateTableRequest{
		Configuration: map[string]string{
			"url":  surrealdbEndpoint,
			"ns":   "test",
			"user": "root",
			"pass": "root",
		},
		SchemaName: "test",
		Table: &pb.Table{
			Name: "txn1",
			Columns: []*pb.Column{
				{Name: "myid", Type: pb.DataType_STRING},
				{Name: "mybool", Type: pb.DataType_BOOLEAN},
				{Name: "myshort", Type: pb.DataType_SHORT},
				{Name: "myint", Type: pb.DataType_INT},
				{Name: "mylong", Type: pb.DataType_LONG},
				{Name: "mydecimal", Type: pb.DataType_DECIMAL},
				{Name: "myfloat", Type: pb.DataType_FLOAT},
				{Name: "mydouble", Type: pb.DataType_DOUBLE},
				{Name: "mynaivedate", Type: pb.DataType_NAIVE_DATE},
				{Name: "mynaivedatetime", Type: pb.DataType_NAIVE_DATETIME},
				{Name: "myutcdatetime", Type: pb.DataType_UTC_DATETIME},
				{Name: "mybinary", Type: pb.DataType_BINARY},
				{Name: "myxml", Type: pb.DataType_XML},
				{Name: "mystring", Type: pb.DataType_STRING},
				{Name: "myjson", Type: pb.DataType_JSON},
				{Name: "mynaivetime", Type: pb.DataType_NAIVE_TIME},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &pb.CreateTableResponse_Success{Success: true}, created.Response)
	described, err := srv.DescribeTable(context.Background(), &pb.DescribeTableRequest{
		Configuration: map[string]string{
			"url":  surrealdbEndpoint,
			"ns":   "test",
			"user": "root",
			"pass": "root",
		},
		SchemaName: "test",
		TableName:  "txn1",
	})
	require.NoError(t, err)
	require.Equal(t, &pb.DescribeTableResponse_Table{
		Table: &pb.Table{
			Name: "txn1",
			Columns: []*pb.Column{
				{Name: "myid", Type: pb.DataType_STRING},
				{Name: "mybool", Type: pb.DataType_BOOLEAN},
				{Name: "myshort", Type: pb.DataType_SHORT},
				{Name: "myint", Type: pb.DataType_INT},
				{Name: "mylong", Type: pb.DataType_LONG},
				{Name: "mydecimal", Type: pb.DataType_DECIMAL},
				{Name: "myfloat", Type: pb.DataType_FLOAT},
				{Name: "mydouble", Type: pb.DataType_DOUBLE},
				{Name: "mynaivedate", Type: pb.DataType_NAIVE_DATE},
				{Name: "mynaivedatetime", Type: pb.DataType_NAIVE_DATETIME},
				{Name: "myutcdatetime", Type: pb.DataType_UTC_DATETIME},
				{Name: "mybinary", Type: pb.DataType_BINARY},
				{Name: "myxml", Type: pb.DataType_XML},
				{Name: "mystring", Type: pb.DataType_STRING},
				{Name: "myjson", Type: pb.DataType_JSON},
				{Name: "mynaivetime", Type: pb.DataType_NAIVE_TIME},
			},
		},
	}, described.Response)
}
