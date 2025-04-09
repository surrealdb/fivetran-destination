# Destination Connector Tests

This directory contains tests for the Fivetran destination connector for SurrealDB.

## Test Structure

The tests are organized in the following structure:

```
destination-data/
└── test-cases/
    ├── case1/
    │   ├── input.json     # Test input data (For the fivetran tester)
    │   └── expected.yaml  # Expected database state (For our db-validator)
    ├── case2/
    │   ├── input.json
    │   └── expected.yaml
    └── ...
```

Each test case consists of:
1. `input.json`: The input data that will be sent to the destination connector
2. `expected.yaml`: The expected state of the SurrealDB database after processing the input

## Running Tests

### Prerequisites

1. Docker installed and running
2. Go 1.22 or later installed (only required for non-Docker runs)
3. Authenticated with Google Artifact Registry for pulling the SDK tester image

### Running All Tests

#### Direct Execution (Go)
To run all test cases directly using Go:

```bash
./destination-connector-test.sh
```

#### Using Docker
To run all test cases using the Docker container:

```bash
USE_DOCKER=true ./destination-connector-test.sh
```

### Running a Specific Test Case

#### Direct Execution (Go)
To run a specific test case directly:

```bash
TEST_CASE=case1 ./destination-connector-test.sh
```

#### Using Docker
To run a specific test case using Docker:

```bash
USE_DOCKER=true TEST_CASE=case1 ./destination-connector-test.sh
```

### Debug Mode
You can enable debug logging with:

```bash
SURREAL_FIVETRAN_DEBUG=1 TEST_CASE=case1 ./destination-connector-test.sh
```

Or with Docker:

```bash
SURREAL_FIVETRAN_DEBUG=1 USE_DOCKER=true TEST_CASE=case1 ./destination-connector-test.sh
```

## Expected Database State Format

See [example.yaml](./db-validator/example.yaml) for the example that illustrates the format.

## Troubleshooting

- If a test fails, check the error message from the db-validator for details about what didn't match
- When using Docker, ensure that the container can reach the SurrealDB instance
- If you encounter Docker-related issues, try cleaning up existing containers:
  ```bash
  docker stop connector-test surrealdb-test || true
  docker rm connector-test surrealdb-test || true
  ```
