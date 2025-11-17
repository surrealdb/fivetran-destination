# SurrealDB Destination Connector for Fivetran

This is a Fivetran [partner-built destination connector](https://fivetran.com/docs/partner-built-program#destinationconnector) for SurrealDB.

The connector enables Fivetran to write data to SurrealDB tables. It implements the Fivetran Destination Connector SDK using gRPC.

## Features

- Writes data to SurrealDB tables
- Supports batch operations
- Handles schema changes (Note though schema changes in SurrealDB are metadata-only)
- Provides data type mapping

## Architecture

The connector implements the `DestinationConnector` gRPC service as defined in [destination_sdk.proto](./proto/destination_sdk.proto) and mainly does two things:

1. Handles connector configuration and validation
2. Handles various write operations to the destination as required by Fivetran

## Configuration

The connector requires the following configuration:

```json
{
    "url": "ws://localhost:8000/rpc",
    "user": "root",
    "pass": "root",
    "ns": "test"
}
```

The configuration is exposed via the connector's ConfigurationForm API and should be provided by the end user.

The connector uses SurrealDB namespace-scoped user/pass sign-in by default.

Please refer to the [setup guide](docs/setup-guide.md) for how to get started with a namespace-level user.

## Development

### Prerequisites

- Go 1.25.3 or later
- Protocol Buffers compiler (protoc)
- SurrealDB instance for testing
- Docker (for running conformance tests)

### Building

#### Local Build
```bash
cd fivetran-destination
go build -o bin/connector
```

#### Docker Build
```bash
# Build the Docker image
docker build -t fivetran-surrealdb-connector .

# Run the container
docker run -p 50052:50052 fivetran-surrealdb-connector
```

The connector will be available on port 50052. Make sure your SurrealDB instance is accessible from the container.

### Testing

#### Unit Tests
```bash
go test ./...
```

#### Conformance Tests
The connector includes a comprehensive set of conformance tests that verify its behavior against Fivetran's requirements. See [tests/README.md](tests/README.md) for detailed instructions on running these tests.

### Running Locally

1. Start the connector:
```bash
./bin/connector --port 50052
```

2. Configure Fivetran to connect to your local instance (refer to Fivetran documentation)

## Implementation Details

### Write Operations

The connector handles different types of write operations:

1. Insert: Creates new records
2. Update: Modifies existing records
3. Delete: Removes records
4. Upsert: Updates or inserts based on primary key

### Data Type Mapping

Automatic mapping between Fivetran and [SurrealDB data types](https://surrealdb.com/docs/surrealql/datamodel#data-types):

| Fivetran Type | SurrealDB Type |
|---------------|----------------|
| STRING        | string         |
| NUMBER        | number         |
| BOOLEAN       | bool           |
| TIMESTAMP     | datetime       |

See [mapping.go](https://github.com/surrealdb/fivetran-destination/blob/main/internal/connector/mapping.go)
for the full list of mappings.

### Error Handling

- Implements transaction rollback on failures
- Provides detailed error reporting
- Handles network interruptions
- Supports retry mechanisms

### Performance Optimization

- Uses batch processing
- Implements connection pooling
- Optimizes write patterns
- Supports parallel processing

## Monitoring

The connector exposes metrics for monitoring:

- Write latency
- Records processed
- Error rates
- Batch sizes
- Transaction success/failure rates

## Troubleshooting

Common issues and solutions:

1. Connection issues
   - Verify SurrealDB credentials
   - Check network connectivity
   - Confirm firewall settings

2. Write failures
   - Check table permissions
   - Verify schema compatibility
   - Review data type mappings

3. Performance issues
   - Adjust batch size
   - Monitor SurrealDB performance
   - Check resource utilization

## Support

For technical issues:
1. Check the troubleshooting guide
2. Review Fivetran documentation
3. Open a GitHub issue
4. Contact Fivetran support for SDK-specific questions 
