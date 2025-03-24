# SurrealDB Destination Connector for Fivetran

This connector enables Fivetran to write data to SurrealDB tables. It implements the Fivetran Destination Connector SDK using gRPC.

## Features

- Writes data to SurrealDB tables
- Supports batch operations
- Handles schema changes
- Provides data type mapping
- Implements efficient write operations
- Supports upsert and delete operations

## Architecture

The connector implements the following gRPC services as defined in `destination_sdk.proto`:

1. `ConfigureService`: Handles connector configuration and validation
2. `DestinationService`: Manages write operations to SurrealDB

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

In near future, we will support token-based authentication, too.

## Development

### Prerequisites

- Go 1.21 or later
- Protocol Buffers compiler (protoc)
- SurrealDB instance for testing

### Building

```bash
cd fivetran-destination
go build -o bin/connector
```

### Testing

```bash
go test ./...
```

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

Automatic mapping between Fivetran and SurrealDB data types:

| Fivetran Type | SurrealDB Type |
|---------------|----------------|
| STRING        | string         |
| NUMBER        | number         |
| BOOLEAN       | bool           |
| TIMESTAMP     | datetime       |

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
