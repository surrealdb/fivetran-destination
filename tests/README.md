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
2. Go 1.22 or later installed
3. Authenticated with Google Artifact Registry for pulling the SDK tester image

### Running All Tests

To run all test cases:

```bash
./destination-connector-test.sh
```

### Running a Specific Test Case

To run a specific test case:

```bash
TEST_CASE=case1 make destination-test
```

You can enable the debug logging with:

```bash
SURREAL_FIVETRAN_DEBUG=1 TEST_CASE=case1 make destination-test
```

## Expected Database State Format

See [example.yaml](./db-validator/example.yaml) for the example that illustrates the format.

## Troubleshooting

- If a test fails, check the error message from the db-validator for details about what didn't match
