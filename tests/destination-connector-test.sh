#!/bin/bash
set -e

SDK_TESTER_TAG=${SDK_TESTER_TAG:-2.25.0311.001}
SURREALDB_TAG=${SURREALDB_TAG:-latest}
TEST_CASE=${TEST_CASE:-all}

# Destination Connector Conformance Test Script
echo "Starting SurrealDB Destination Connector Conformance Test"

# Create data directory if it doesn't exist
mkdir -p "$(pwd)/destination-data"

# Check if SurrealDB is running
echo "Checking if SurrealDB is running..."
if ! curl -s http://localhost:8000/health > /dev/null; then
    echo "SurrealDB is not running. Starting SurrealDB..."
    docker run -d --name surrealdb-test -p 8000:8000 surrealdb/surrealdb:$SURREALDB_TAG start --user root --pass root
    sleep 5
    echo "SurrealDB started."
else
    echo "SurrealDB is already running."
fi

# Function to run a single test case
run_test_case() {
    local case_dir="$1"
    local case_name="$(basename "$case_dir")"
    echo "Running test case: $case_name"

    # Copy input file to destination-data with new name
    cp "$case_dir/input.json" "$(pwd)/destination-data/input_${case_name}.json"

    # Build and start the destination connector
    echo "Building and starting the destination connector..."
    cd "$(pwd)/.."
    go build -o bin/connector
    ./bin/connector --port 50052 &
    CONNECTOR_PID=$!
    cd tests

    # Wait for the connector to start
    echo "Waiting for the connector to start..."
    sleep 5

    # Check if Docker is authenticated with Google Artifact Registry
    echo "Checking Docker authentication with Google Artifact Registry..."
    if ! docker pull us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$SDK_TESTER_TAG > /dev/null 2>&1; then
        echo "Docker is not authenticated with Google Artifact Registry."
        echo "Please run: gcloud auth configure-docker us-docker.pkg.dev"
        exit 1
    fi

    # Run the destination connector tester
    echo "Running the destination connector tester..."
    docker run --mount type=bind,source="$(pwd)/destination-data",target=/data \
      -a STDIN -a STDOUT -a STDERR -it \
      -e WORKING_DIR="$(pwd)/destination-data" \
      -e GRPC_HOSTNAME=host.docker.internal --network=host \
      us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$SDK_TESTER_TAG \
      --tester-type destination --port 50052 --input-file "input_${case_name}.json"

    # Run the db-validator
    echo "Validating database state..."
    cd db-validator
    go build -o ../bin/db-validator
    cd ..
    ./bin/db-validator "$case_dir/expected.yaml"

    # Clean up
    echo "Cleaning up..."
    kill $CONNECTOR_PID

    echo "Test case $case_name completed successfully!"
}

# Function to run all test cases
run_all_test_cases() {
    local test_cases_dir="$(pwd)/destination-data/test-cases"
    for case_dir in "$test_cases_dir"/*/; do
        if [ -d "$case_dir" ]; then
            run_test_case "$case_dir"
        fi
    done
}

# Run the appropriate test cases
if [ "$TEST_CASE" = "all" ]; then
    run_all_test_cases
else
    case_dir="$(pwd)/destination-data/test-cases/$TEST_CASE"
    if [ ! -d "$case_dir" ]; then
        echo "Test case directory not found: $case_dir"
        exit 1
    fi
    run_test_case "$case_dir"
fi

echo "All tests completed successfully!" 