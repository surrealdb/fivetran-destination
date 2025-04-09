#!/bin/bash
set -e

SDK_TESTER_TAG=${SDK_TESTER_TAG:-2.25.0311.001}
SURREALDB_TAG=${SURREALDB_TAG:-latest}
TEST_CASE=${TEST_CASE:-all}
USE_DOCKER=${USE_DOCKER:-false}
LOG_DIR="$(pwd)/logs"

# Destination Connector Conformance Test Script
echo "Starting SurrealDB Destination Connector Conformance Test"

# Create data directory if it doesn't exist
mkdir -p "$(pwd)/destination-data"

# Check if SurrealDB is running
echo "Checking if SurrealDB is running..."
if ! curl -s http://localhost:8000/health > /dev/null; then
    echo "SurrealDB is not running. Starting SurrealDB..."
    docker run -d --name surrealdb-test \
      -p 8000:8000 \
      surrealdb/surrealdb:$SURREALDB_TAG start --user root --pass root
    sleep 5
    echo "SurrealDB started."
else
    echo "SurrealDB is already running."
fi

# Function to start the connector
start_connector() {
    if [ "$USE_DOCKER" = "true" ]; then
        echo "Starting connector via Docker..."
        # Build the Docker image if it doesn't exist
        if ! docker image inspect fivetran-surrealdb-connector:latest > /dev/null 2>&1; then
            cd "$(pwd)/.."
            docker build -t fivetran-surrealdb-connector .
            cd tests
        fi
        # Run the container with environment variables and network settings
        #
        # Note that this exposes 50052 to the host thanks to `--network host` and the --port flag
        # specified in the Dockerfile
        #
        # Also noet that the mount target needs to be the absolute path to the destination-data directory
        # rather than "/data", because it needs to correlate with the WORKING_DIR environment variable
        # specified in the destination connector tester container!
        docker run -d --name connector-test \
            --mount type=bind,source="$(pwd)/destination-data",target="$(pwd)/destination-data" \
            --network host \
            -e SURREAL_FIVETRAN_DEBUG="${SURREAL_FIVETRAN_DEBUG:-}" \
            -p 50052:50052 \
            fivetran-surrealdb-connector
        CONNECTOR_PID="docker"
    else
        echo "Starting connector directly..."
        cd "$(pwd)/.."
        go build -o bin/connector
        SURREAL_FIVETRAN_DEBUG="${SURREAL_FIVETRAN_DEBUG:-}" ./bin/connector --port 50052 &
        CONNECTOR_PID=$!
        cd tests
    fi
}

# Function to stop the connector
stop_connector() {
    if [ "$USE_DOCKER" = "true" ]; then
        echo "Stopping connector container..."
        docker stop connector-test
        docker rm connector-test
    else
        echo "Stopping connector process..."
        kill $CONNECTOR_PID
    fi
}

# Function to clean up Docker resources
cleanup_docker() {
    if [ "$USE_DOCKER" = "true" ]; then
        echo "Cleaning up Docker resources..."
        docker stop connector-test surrealdb-test || true
        docker rm connector-test surrealdb-test || true
    fi
}

# Set up trap to clean up Docker resources on script exit
trap cleanup_docker EXIT

# Function to dump connector logs
dump_connector_logs() {
    local case_name="$1"
    local log_file="${LOG_DIR}/connector_${case_name}_$(date +%Y%m%d_%H%M%S).log"

    if [ "$USE_DOCKER" = "true" ]; then
        echo "Dumping connector logs to $log_file..."
        mkdir -p "$LOG_DIR"
        docker logs connector-test > "$log_file" 2>&1
        echo "Connector logs saved to $log_file"
    fi
}

# Function to run a single test case
run_test_case() {
    local case_dir="$1"
    local case_name="$(basename "$case_dir")"
    echo "Running test case: $case_name"

    # Copy input file to destination-data with new name
    cp "$case_dir/input.json" "$(pwd)/destination-data/input_${case_name}.json"

    # Start the connector
    start_connector

    # Wait for the connector to start
    echo "Waiting for the connector to start..."
    sleep 5

    # Build and run db-truncate to clean up tables
    echo "Building and running db-truncate..."
    cd db-truncate
    go build -o ../bin/db-truncate
    cd ..
    SURREALDB_NAMESPACE=testns SURREALDB_DATABASE=tester ./bin/db-truncate -f "$case_dir/expected.yaml"
    echo "Tables truncated successfully."

    # Check if Docker is authenticated with Google Artifact Registry
    echo "Checking Docker authentication with Google Artifact Registry..."
    if ! docker pull us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$SDK_TESTER_TAG > /dev/null 2>&1; then
        echo "Docker is not authenticated with Google Artifact Registry."
        echo "Please run: gcloud auth configure-docker us-docker.pkg.dev"
        exit 1
    fi

    GRPC_HOSTNAME="host.docker.internal"
    if [ "$USE_DOCKER" = "true" ]; then
        GRPC_HOSTNAME="localhost"
   fi

    # Run the destination connector tester
    echo "Running the destination connector tester..."
    docker run --mount type=bind,source="$(pwd)/destination-data",target=/data \
      -a STDIN -a STDOUT -a STDERR -it \
      -e WORKING_DIR="$(pwd)/destination-data" \
      --network=host \
      -e GRPC_HOSTNAME=$GRPC_HOSTNAME \
      us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$SDK_TESTER_TAG \
      --tester-type destination --port 50052 --input-file "input_${case_name}.json"

    # Run the db-validator
    echo "Validating database state..."
    cd db-validator
    go build -o ../bin/db-validator
    cd ..
    
    # Store the validation result
    local validation_result=0
    SURREALDB_NAMESPACE=testns SURREALDB_DATABASE=tester ./bin/db-validator "$case_dir/expected.yaml" || validation_result=$?

    # Always dump logs regardless of validation result
    echo "Dumping connector logs..."
    dump_connector_logs "$case_name"
    stop_connector

    # Exit with validation result
    if [ $validation_result -ne 0 ]; then
        echo "Test case $case_name failed validation!"
        exit $validation_result
    fi

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