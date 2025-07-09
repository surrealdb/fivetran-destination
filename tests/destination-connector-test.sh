#!/bin/bash
set -e

# Enable debug mode if DEBUG is set
if [ -n "$DEBUG" ]; then
    set -x
fi

SDK_TESTER_TAG=${SDK_TESTER_TAG:-2.25.0311.001}
SURREALDB_TAG=${SURREALDB_TAG:-latest}
TEST_CASE=${TEST_CASE:-all}
USE_DOCKER=${USE_DOCKER:-false}
LOG_DIR="$(pwd)/logs"
SKIP_CLEANUP=${SKIP_CLEANUP:-false}

# Global variables to track connector PID and log files
CONNECTOR_PID=""
CURRENT_CONNECTOR_LOG=""
CURRENT_TESTER_LOG=""
CURRENT_VALIDATOR_LOG=""

# Function to clean up Docker resources and processes
cleanup_all() {
    if [ "$SKIP_CLEANUP" = "true" ]; then
        echo "Skipping cleanup (SKIP_CLEANUP=true)"
        return
    fi
    
    echo "Cleaning up resources..."
    
    # Kill any existing connector processes on port 50052
    echo "Checking for existing connector processes..."
    CONNECTOR_PIDS=$(lsof -ti:50052 2>/dev/null || true)
    if [ -n "$CONNECTOR_PIDS" ]; then
        echo "Found connector process(es) on port 50052: $CONNECTOR_PIDS"
        echo "Killing existing connector process(es)..."
        kill -9 $CONNECTOR_PIDS 2>/dev/null || true
        sleep 1
    fi
    
    # Stop and remove connector-test container if exists
    if docker ps -a --format '{{.Names}}' | grep -q '^connector-test$'; then
        echo "Stopping and removing connector-test container..."
        docker stop connector-test >/dev/null 2>&1 || true
        docker rm connector-test >/dev/null 2>&1 || true
    fi
    
    # Only stop local SurrealDB if we're not using a remote instance
    if [ -z "$SURREALDB_ENDPOINT" ] || [ -z "$SURREALDB_TOKEN" ]; then
        if docker ps -a --format '{{.Names}}' | grep -q '^surrealdb-test$'; then
            echo "Stopping and removing surrealdb-test container..."
            docker stop surrealdb-test >/dev/null 2>&1 || true
            docker rm surrealdb-test >/dev/null 2>&1 || true
        fi
    fi
}

# Destination Connector Conformance Test Script
echo "Starting SurrealDB Destination Connector Conformance Test"
echo "Environment variables:"
echo "  SKIP_CLEANUP=$SKIP_CLEANUP (set to 'true' to keep containers after test)"
echo "  USE_DOCKER=$USE_DOCKER"
echo "  TEST_CASE=$TEST_CASE"
echo ""

# Save the original directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "Working directory: $(pwd)"

# Clean up any leftover containers and processes from previous runs
cleanup_all

# Create data directory if it doesn't exist
mkdir -p "$(pwd)/destination-data"

# Check if we can access the SDK tester image
echo "Checking access to SDK tester image..."
echo "Pulling image: us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$SDK_TESTER_TAG"
if ! docker pull us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$SDK_TESTER_TAG; then
    echo "ERROR: Failed to pull SDK tester image."
    echo "This image is required to run the tests."
    echo "Please ensure you have internet connectivity and Docker is running."
    exit 1
fi
echo "SDK tester image ready."

# Check if we're using a remote SurrealDB instance
if [ -n "$SURREALDB_ENDPOINT" ] && [ -n "$SURREALDB_TOKEN" ]; then
    echo "Using remote SurrealDB instance at $SURREALDB_ENDPOINT"
    # Check if remote SurrealDB is accessible
    echo "Checking if remote SurrealDB is accessible..."
    if ! docker run --rm surrealdb/surrealdb:$SURREALDB_TAG isready --endpoint "$SURREALDB_ENDPOINT" > /dev/null 2>&1; then
        echo "Remote SurrealDB is not accessible. Please check your connection and credentials."
        exit 1
    fi
    echo "Remote SurrealDB is accessible."

    # Generate configuration.json for remote instance
    echo "Generating configuration.json for remote instance..."
    # Ensure endpoint has /rpc suffix
    if [[ ! "$SURREALDB_ENDPOINT" == */rpc ]]; then
        SURREALDB_ENDPOINT="${SURREALDB_ENDPOINT}/rpc"
    fi
    cat > "$(pwd)/destination-data/configuration.json" << EOF
{
  "url": "$SURREALDB_ENDPOINT",
  "ns": "${SURREALDB_NAMESPACE:-testns}",
  "db": "${SURREALDB_DATABASE:-tester}",
  "token": "$SURREALDB_TOKEN"
}
EOF
else
    # Check if local SurrealDB is running
    echo "Checking if local SurrealDB is running..."
    
    # First, check if a container named surrealdb-test exists
    if docker ps -a --format '{{.Names}}' | grep -q '^surrealdb-test$'; then
        echo "Found existing surrealdb-test container. Checking its status..."
        if docker ps --format '{{.Names}}' | grep -q '^surrealdb-test$'; then
            echo "SurrealDB container is already running."
        else
            echo "Starting existing SurrealDB container..."
            docker start surrealdb-test
            sleep 5
            echo "SurrealDB container started."
        fi
    else
        # No existing container, check if SurrealDB is accessible
        if ! docker run --rm surrealdb/surrealdb:$SURREALDB_TAG isready --endpoint http://localhost:8000 > /dev/null 2>&1; then
            echo "SurrealDB is not running. Starting new SurrealDB container..."
            docker run -d --name surrealdb-test \
              -p 8000:8000 \
              surrealdb/surrealdb:$SURREALDB_TAG start --user root --pass root
            sleep 5
            echo "SurrealDB started."
        else
            echo "SurrealDB is already running (not managed by this script)."
        fi
    fi

    # Generate configuration.json for local instance
    echo "Generating configuration.json for local instance..."
    # Use container name if running connector in Docker
    if [ "$USE_DOCKER" = "true" ]; then
        SURREALDB_HOST="surrealdb-test"
    else
        SURREALDB_HOST="localhost"
    fi
    cat > "$(pwd)/destination-data/configuration.json" << EOF
{
  "url": "ws://${SURREALDB_HOST}:8000/rpc",
  "ns": "${SURREALDB_NAMESPACE:-testns}",
  "db": "${SURREALDB_DATABASE:-tester}",
  "user": "root",
  "pass": "root"
}
EOF
fi

# Function to start the connector
start_connector() {
    local case_name="$1"
    if [ "$USE_DOCKER" = "true" ]; then
        echo "Starting connector via Docker..."
        pushd ".." > /dev/null
        docker build -t fivetran-surrealdb-connector .
        popd > /dev/null
        
        # Remove existing connector-test container if it exists
        if docker ps -a --format '{{.Names}}' | grep -q '^connector-test$'; then
            echo "Removing existing connector-test container..."
            docker stop connector-test >/dev/null 2>&1 || true
            docker rm connector-test >/dev/null 2>&1 || true
        fi
        
        # Run the container with environment variables and network settings
        docker run -d --name connector-test \
            --mount type=bind,source="$(pwd)/destination-data",target="/workspace/tests/destination-data" \
            --link surrealdb-test:surrealdb-test \
            -e SURREAL_FIVETRAN_DEBUG="${SURREAL_FIVETRAN_DEBUG:-}" \
            -e SURREALDB_ENDPOINT="${SURREALDB_ENDPOINT:-}" \
            -e SURREALDB_TOKEN="${SURREALDB_TOKEN:-}" \
            -p 50052:50052 \
            fivetran-surrealdb-connector
        CONNECTOR_PID="docker"
    else
        echo "Starting connector directly..."
        pushd ".." > /dev/null
        go build -o bin/connector
        SURREAL_FIVETRAN_DEBUG="${SURREAL_FIVETRAN_DEBUG:-}" \
        SURREALDB_ENDPOINT="${SURREALDB_ENDPOINT:-}" \
        SURREALDB_TOKEN="${SURREALDB_TOKEN:-}" \
        ./bin/connector --port 50052 > "$CURRENT_CONNECTOR_LOG" 2>&1 &
        CONNECTOR_PID=$!
        popd > /dev/null
    fi
}

# Function to stop the connector
stop_connector() {
    if [ "$USE_DOCKER" = "true" ]; then
        echo "Stopping connector container..."
        docker stop connector-test >/dev/null 2>&1 || true
        docker rm connector-test >/dev/null 2>&1 || true
    else
        echo "Stopping connector process..."
        if [ -n "$CONNECTOR_PID" ] && [ "$CONNECTOR_PID" != "0" ]; then
            # Try graceful shutdown first
            kill $CONNECTOR_PID 2>/dev/null || true
            sleep 1
            # Force kill if still running
            kill -9 $CONNECTOR_PID 2>/dev/null || true
        fi
    fi
}

# Set up trap to clean up resources on script exit (including Ctrl+C)
trap 'echo "Caught signal, cleaning up..."; cleanup_all; exit 130' INT TERM
trap cleanup_all EXIT

# Function to dump connector logs
dump_connector_logs() {
    local case_name="$1"
    
    echo "Saving test logs..."
    
    if [ "$USE_DOCKER" = "true" ]; then
        # For Docker mode, dump the connector logs
        docker logs connector-test > "${LOG_DIR}/${case_name}_connector.log" 2>&1
    else
        # For non-Docker mode, the logs are already being written to the file
        # Just ensure the file exists
        if [ ! -f "${LOG_DIR}/${case_name}_connector.log" ]; then
            echo "[No connector logs captured]" > "${LOG_DIR}/${case_name}_connector.log"
        fi
    fi
    
    # Ensure tester log exists
    if [ ! -f "${LOG_DIR}/${case_name}_tester.log" ]; then
        echo "[No tester logs captured]" > "${LOG_DIR}/${case_name}_tester.log"
    fi
    
    # Ensure validator log exists
    if [ ! -f "${LOG_DIR}/${case_name}_validator.log" ]; then
        echo "[No validator logs captured]" > "${LOG_DIR}/${case_name}_validator.log"
    fi
}

# Function to run a single test case
run_test_case() {
    local case_dir="$1"
    local case_name="$(basename "$case_dir")"
    echo "Running test case: $case_name"

    # Copy input file to destination-data with new name
    cp "$case_dir/input.json" "$(pwd)/destination-data/input_${case_name}.json"

    # Set the log files for this test case
    CURRENT_CONNECTOR_LOG="${LOG_DIR}/${case_name}_connector.log"
    CURRENT_TESTER_LOG="${LOG_DIR}/${case_name}_tester.log"
    CURRENT_VALIDATOR_LOG="${LOG_DIR}/${case_name}_validator.log"
    mkdir -p "$LOG_DIR"
    
    # Start the connector
    start_connector "$case_name"

    # Wait for the connector to start
    echo "Waiting for the connector to start..."
    sleep 5

    # Build and run db-truncate to clean up tables
    echo "Building and running db-truncate..."
    pushd db-truncate > /dev/null
    go build -o ../bin/db-truncate
    popd > /dev/null
    SURREALDB_NAMESPACE=testns SURREALDB_DATABASE=tester \
    SURREALDB_ENDPOINT="${SURREALDB_ENDPOINT:-}" \
    SURREALDB_TOKEN="${SURREALDB_TOKEN:-}" \
    ./bin/db-truncate -f "$case_dir/expected.yaml"
    echo "Tables truncated successfully."


    # Determine the correct hostname for gRPC connection
    if [ "$USE_DOCKER" = "true" ]; then
        # Both connector and tester are in Docker, use container name
        GRPC_HOSTNAME="connector-test"
        echo "Connector running in Docker, using container name: connector-test"
    else
        # Connector is running directly in the devcontainer
        # For DIND, containers can reach the host via the Docker bridge gateway
        DOCKER_GATEWAY=$(docker network inspect bridge --format '{{range .IPAM.Config}}{{.Gateway}}{{end}}')
        if [ -n "$DOCKER_GATEWAY" ]; then
            GRPC_HOSTNAME="$DOCKER_GATEWAY"
            echo "Connector running in devcontainer, using Docker gateway: $DOCKER_GATEWAY"
        else
            # Fallback if we can't get the gateway
            GRPC_HOSTNAME="172.17.0.1"
            echo "Could not detect Docker gateway, using default: $GRPC_HOSTNAME"
        fi
    fi
    
    echo "Using GRPC_HOSTNAME=$GRPC_HOSTNAME for connector at port 50052"

    # Run the destination connector tester
    echo "Running the destination connector tester..."
    
    # Run the SDK tester container and capture its output
    # When USE_DOCKER=false (default), the connector runs in devcontainer and
    # the tester needs to connect via Docker bridge gateway
    # When USE_DOCKER=true, link to the connector container
    DOCKER_LINK_ARG=""
    if [ "$USE_DOCKER" = "true" ]; then
        DOCKER_LINK_ARG="--link connector-test:connector-test"
    fi

    # Check if we're running in an interactive terminal
    DOCKER_TTY_FLAG=""
    if [ -t 0 ] && [ -t 1 ]; then
        DOCKER_TTY_FLAG="-it"
    fi

    docker run --mount type=bind,source="$(pwd)/destination-data",target=/data \
      -a STDIN -a STDOUT -a STDERR $DOCKER_TTY_FLAG \
      $DOCKER_LINK_ARG \
      -e WORKING_DIR="$(pwd)/destination-data" \
      -e GRPC_HOSTNAME=$GRPC_HOSTNAME \
      us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$SDK_TESTER_TAG \
      --tester-type destination --port 50052 --input-file "input_${case_name}.json" \
      2>&1 | tee "$CURRENT_TESTER_LOG"

    # Run the db-validator
    echo "Validating database state..."
    pushd db-validator > /dev/null
    go build -o ../bin/db-validator
    popd > /dev/null
    
    # Store the validation result
    local validation_result=0
    SURREALDB_NAMESPACE=testns SURREALDB_DATABASE=tester \
    SURREALDB_ENDPOINT="${SURREALDB_ENDPOINT:-}" \
    SURREALDB_TOKEN="${SURREALDB_TOKEN:-}" \
    ./bin/db-validator "$case_dir/expected.yaml" 2>&1 | tee "$CURRENT_VALIDATOR_LOG"
    validation_result=${PIPESTATUS[0]}

    # Always dump logs regardless of validation result
    echo "Saving test logs..."
    dump_connector_logs "$case_name"
    stop_connector

    # Return validation result (don't exit the whole script)
    if [ $validation_result -ne 0 ]; then
        echo "Test case $case_name failed validation!"
        return $validation_result
    fi

    echo "Test case $case_name completed successfully!"
    return 0
}

# Function to run all test cases
run_all_test_cases() {
    local test_cases_dir="$(pwd)/destination-data/test-cases"
    echo "Looking for test cases in: $test_cases_dir"
    
    if [ ! -d "$test_cases_dir" ]; then
        echo "ERROR: Test cases directory not found: $test_cases_dir"
        exit 1
    fi
    
    local found_cases=0
    local passed_cases=0
    local failed_cases=0
    
    # Arrays to store passed and failed test names
    local passed_tests=()
    local failed_tests=()
    
    # Associative array to store log files for each test
    declare -A test_log_files
    
    for case_dir in "$test_cases_dir"/*/; do
        if [ -d "$case_dir" ]; then
            # Check if the test case has required files
            if [ -f "$case_dir/input.json" ] && [ -f "$case_dir/expected.yaml" ]; then
                found_cases=$((found_cases + 1))
                local case_name=$(basename "$case_dir")
                echo ""
                echo "========================================="
                echo "Test case $found_cases of $(ls -d "$test_cases_dir"/*/ | wc -l): $case_name"
                echo "========================================="
                
                if run_test_case "$case_dir"; then
                    passed_cases=$((passed_cases + 1))
                    passed_tests+=("$case_name")
                    test_log_files["$case_name"]="${LOG_DIR}/${case_name}"
                else
                    failed_cases=$((failed_cases + 1))
                    failed_tests+=("$case_name")
                    test_log_files["$case_name"]="${LOG_DIR}/${case_name}"
                fi
            else
                echo "Skipping $case_dir - missing input.json or expected.yaml"
            fi
        fi
    done
    
    if [ $found_cases -eq 0 ]; then
        echo "ERROR: No valid test cases found in $test_cases_dir"
        echo "Each test case directory must contain input.json and expected.yaml"
        exit 1
    fi
    
    echo ""
    echo "========================================="
    echo "TEST SUMMARY"
    echo "========================================="
    echo "Total test cases: $found_cases"
    echo ""
    
    if [ $passed_cases -gt 0 ]; then
        echo "PASSED ($passed_cases):"
        for test in "${passed_tests[@]}"; do
            echo "  ✓ $test"
        done
    fi
    
    if [ $failed_cases -gt 0 ]; then
        echo ""
        echo "FAILED ($failed_cases):"
        for test in "${failed_tests[@]}"; do
            echo "  ✗ $test"
        done
        
        echo ""
        echo "To view logs for failed tests:"
        for test in "${failed_tests[@]}"; do
            log_prefix="${test_log_files[$test]}"
            if [ -f "${log_prefix}_connector.log" ] || [ -f "${log_prefix}_tester.log" ] || [ -f "${log_prefix}_validator.log" ]; then
                echo "  # Logs for $test:"
                [ -f "${log_prefix}_connector.log" ] && echo "    cat ${log_prefix}_connector.log  # connector logs"
                [ -f "${log_prefix}_tester.log" ] && echo "    cat ${log_prefix}_tester.log     # tester logs"
                [ -f "${log_prefix}_validator.log" ] && echo "    cat ${log_prefix}_validator.log  # validator logs"
                echo "    # View all logs together:"
                echo "    tail -n +1 ${log_prefix}_*.log       # all logs with headers"
            fi
        done
    fi
    
    echo ""
    echo "All test logs are saved in: $LOG_DIR/"
    echo ""
    echo "Tip: To search for errors across all logs:"
    echo "  grep -r ERROR $LOG_DIR/"
    echo "========================================="
    
    if [ $failed_cases -gt 0 ]; then
        exit 1
    fi
}

# Run the appropriate test cases
echo "Running test case(s): $TEST_CASE"
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

echo ""
echo "========================================="
echo "All tests completed successfully!"
echo "=========================================" 