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
METRICS_LOG_INTERVAL=${METRICS_LOG_INTERVAL:-5s}
ANALYZE_METRICS=${ANALYZE_METRICS:-true}

# Global variables to track connector PID and log files
CONNECTOR_PID=""
TIMESTAMP_PID=""
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

# Helper function to extract metric value from JSON or key=value format
extract_metric() {
    local lines="$1"
    local metric_name="$2"

    # Try JSON format first (handling timestamps and spaces)
    local values=$(echo "$lines" | grep -o "\"${metric_name}\":[0-9.]*" | cut -d: -f2 | tr -d ',')
    if [ -z "$values" ]; then
        # Try key=value format
        values=$(echo "$lines" | grep -o "${metric_name}=[0-9.]*" | cut -d= -f2)
    fi
    echo "$values"
}

# Function to extract and analyze metrics from connector logs
analyze_metrics() {
    local log_file="$1"
    local case_name="$2"
    local metrics_file="${LOG_DIR}/${case_name}_metrics_analysis.txt"

    echo "Analyzing metrics for test case: $case_name" > "$metrics_file"
    echo "================================================" >> "$metrics_file"
    echo "" >> "$metrics_file"

    # Extract all metrics lines
    local metrics_lines=$(grep "Connector Performance Metrics" "$log_file" 2>/dev/null || true)

    if [ -z "$metrics_lines" ]; then
        echo "No metrics found in connector logs" >> "$metrics_file"
        return
    fi

    # Count total metrics logs
    local count=$(echo "$metrics_lines" | wc -l)
    echo "Total metrics logs captured: $count" >> "$metrics_file"
    echo "" >> "$metrics_file"

    # Extract and analyze each metric
    echo "=== Records Processing ===" >> "$metrics_file"
    local records_per_second=$(extract_metric "$metrics_lines" "records_per_second")
    if [ -n "$records_per_second" ]; then
        # Filter out zero values for more meaningful statistics
        local non_zero_rps=$(echo "$records_per_second" | awk '{if($1>0) print $1}')
        if [ -n "$non_zero_rps" ]; then
            local avg_rps=$(echo "$non_zero_rps" | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
            local max_rps=$(echo "$non_zero_rps" | awk 'BEGIN{max=0} {if($1>max) max=$1} END{print max}')
            local min_rps=$(echo "$non_zero_rps" | awk 'BEGIN{min=999999999} {if($1<min && $1>0) min=$1} END{if(min==999999999) print 0; else print min}')
            echo "Records per second - Avg: $avg_rps, Max: $max_rps, Min: $min_rps" >> "$metrics_file"
        else
            echo "Records per second - No non-zero values recorded" >> "$metrics_file"
        fi
    fi

    echo "" >> "$metrics_file"
    echo "=== Throughput ===" >> "$metrics_file"
    local mb_per_second=$(extract_metric "$metrics_lines" "mb_per_second")
    if [ -n "$mb_per_second" ]; then
        local avg_mbps=$(echo "$mb_per_second" | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
        local max_mbps=$(echo "$mb_per_second" | awk 'BEGIN{max=0} {if($1>max) max=$1} END{print max}')
        echo "MB per second - Avg: $avg_mbps, Max: $max_mbps" >> "$metrics_file"
    fi

    echo "" >> "$metrics_file"
    echo "=== Database Operations ===" >> "$metrics_file"
    local db_writes_per_second=$(extract_metric "$metrics_lines" "db_writes_per_second")
    if [ -n "$db_writes_per_second" ]; then
        local avg_dbwps=$(echo "$db_writes_per_second" | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
        local max_dbwps=$(echo "$db_writes_per_second" | awk 'BEGIN{max=0} {if($1>max) max=$1} END{print max}')
        echo "DB writes per second - Avg: $avg_dbwps, Max: $max_dbwps" >> "$metrics_file"
    fi

    echo "" >> "$metrics_file"
    echo "=== File Processing Performance ===" >> "$metrics_file"
    local avg_file_ms=$(extract_metric "$metrics_lines" "avg_file_processing_ms")
    if [ -n "$avg_file_ms" ]; then
        local overall_avg_ms=$(echo "$avg_file_ms" | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
        local max_avg_ms=$(echo "$avg_file_ms" | awk 'BEGIN{max=0} {if($1>max) max=$1} END{print max}')
        echo "Average file processing time (ms) - Avg: $overall_avg_ms, Max: $max_avg_ms" >> "$metrics_file"
    fi

    echo "" >> "$metrics_file"
    echo "=== Resource Usage ===" >> "$metrics_file"
    local cpu_usage=$(extract_metric "$metrics_lines" "cpu_usage_percent")
    if [ -n "$cpu_usage" ]; then
        local avg_cpu=$(echo "$cpu_usage" | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
        local max_cpu=$(echo "$cpu_usage" | awk 'BEGIN{max=0} {if($1>max) max=$1} END{print max}')
        echo "CPU usage % - Avg: $avg_cpu, Max: $max_cpu" >> "$metrics_file"
    fi

    local memory_mb=$(extract_metric "$metrics_lines" "memory_usage_mb")
    if [ -n "$memory_mb" ]; then
        local avg_mem=$(echo "$memory_mb" | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
        local max_mem=$(echo "$memory_mb" | awk 'BEGIN{max=0} {if($1>max) max=$1} END{print max}')
        echo "Memory usage MB - Avg: $avg_mem, Max: $max_mem" >> "$metrics_file"
    fi

    echo "" >> "$metrics_file"
    echo "=== Errors ===" >> "$metrics_file"
    local file_processing_errors=$(extract_metric "$metrics_lines" "file_processing_errors")
    local db_write_errors=$(extract_metric "$metrics_lines" "db_write_errors")
    local total_file_errors=$(echo "$file_processing_errors" | awk '{sum+=$1} END{print sum}')
    local total_db_errors=$(echo "$db_write_errors" | awk '{sum+=$1} END{print sum}')
    echo "Total file processing errors: ${total_file_errors:-0}" >> "$metrics_file"
    echo "Total DB write errors: ${total_db_errors:-0}" >> "$metrics_file"

    echo "" >> "$metrics_file"
    echo "=== Full Metrics Logs ===" >> "$metrics_file"
    echo "$metrics_lines" >> "$metrics_file"

    # Print summary to console
    echo "  Metrics Analysis Summary for $case_name:"
    echo "    - Metrics logs captured: $count"

    # Only show non-zero performance metrics in summary
    if [ -n "$non_zero_rps" ] && [ -n "$avg_rps" ] && [ "$avg_rps" != "0" ]; then
        echo "    - Avg records/sec: $avg_rps (max: $max_rps)"
    fi
    if [ -n "$mb_per_second" ]; then
        local non_zero_mbps=$(echo "$mb_per_second" | awk '{if($1>0) print $1}')
        if [ -n "$non_zero_mbps" ] && [ -n "$avg_mbps" ] && [ "$avg_mbps" != "0" ]; then
            echo "    - Avg MB/sec: $avg_mbps (max: $max_mbps)"
        fi
    fi
    if [ -n "$avg_op_ms" ]; then
        local non_zero_ms=$(echo "$avg_op_ms" | awk '{if($1>0) print $1}')
        if [ -n "$non_zero_ms" ] && [ -n "$overall_avg_ms" ] && [ "$overall_avg_ms" != "0" ]; then
            echo "    - Avg operation time: ${overall_avg_ms}ms"
        fi
    fi

    # Always show error count (even if zero) as it's a key indicator
    local total_errors=$((${total_op_errors:-0} + ${total_db_errors:-0}))
    echo "    - Total errors: $total_errors"

    echo "    - Full analysis saved to: $metrics_file"
}

# Destination Connector Conformance Test Script
echo "Starting SurrealDB Destination Connector Conformance Test"
echo "Environment variables:"
echo "  SKIP_CLEANUP=$SKIP_CLEANUP (set to 'true' to keep containers after test)"
echo "  USE_DOCKER=$USE_DOCKER"
echo "  TEST_CASE=$TEST_CASE"
echo "  METRICS_LOG_INTERVAL=$METRICS_LOG_INTERVAL"
echo "  ANALYZE_METRICS=$ANALYZE_METRICS"
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

CONNECTOR_WORKING_DIR=""
if [ "$USE_DOCKER" = "true" ]; then
    CONNECTOR_WORKING_DIR="/data"
else
    CONNECTOR_WORKING_DIR="$(pwd)/destination-data"
fi

# Function to start the connector
start_connector() {
    local case_name="$1"

    # Ensure port 50052 is free before starting
    local existing_pids=$(lsof -ti:50052 2>/dev/null || true)
    if [ -n "$existing_pids" ]; then
        echo "Warning: Found process already using port 50052, killing it..."
        kill -9 $existing_pids 2>/dev/null || true
        sleep 1
    fi

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
            --mount type=bind,source="$(pwd)/destination-data",target="${CONNECTOR_WORKING_DIR}" \
            --link surrealdb-test:surrealdb-test \
            -e SURREAL_FIVETRAN_DEBUG="${SURREAL_FIVETRAN_DEBUG:-}" \
            -e SURREALDB_ENDPOINT="${SURREALDB_ENDPOINT:-}" \
            -e SURREALDB_TOKEN="${SURREALDB_TOKEN:-}" \
            -e METRICS_LOG_INTERVAL="${METRICS_LOG_INTERVAL}" \
            -p 50052:50052 \
            fivetran-surrealdb-connector
        CONNECTOR_PID="docker"
    else
        echo "Starting connector directly..."
        pushd ".." > /dev/null
        go build -o bin/connector
        # Start the connector and capture its PID directly
        SURREAL_FIVETRAN_DEBUG="${SURREAL_FIVETRAN_DEBUG:-}" \
        SURREALDB_ENDPOINT="${SURREALDB_ENDPOINT:-}" \
        SURREALDB_TOKEN="${SURREALDB_TOKEN:-}" \
        METRICS_LOG_INTERVAL="${METRICS_LOG_INTERVAL}" \
        ./bin/connector --port 50052 > "$CURRENT_CONNECTOR_LOG.raw" 2>&1 &
        CONNECTOR_PID=$!

        # Add timestamps to the log in background
        tail -f "$CURRENT_CONNECTOR_LOG.raw" 2>/dev/null | while IFS= read -r line; do 
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] $line"
        done > "$CURRENT_CONNECTOR_LOG" &
        TIMESTAMP_PID=$!
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

        # Stop the timestamp process first
        if [ -n "$TIMESTAMP_PID" ] && [ "$TIMESTAMP_PID" != "0" ]; then
            kill $TIMESTAMP_PID 2>/dev/null || true
        fi

        # Stop the connector using the saved PID
        if [ -n "$CONNECTOR_PID" ] && [ "$CONNECTOR_PID" != "0" ]; then
            # Try graceful shutdown first
            kill $CONNECTOR_PID 2>/dev/null || true
            sleep 1
            # Force kill if still running
            kill -9 $CONNECTOR_PID 2>/dev/null || true
        fi

        # Double-check by looking for any process on port 50052
        local remaining_pids=$(lsof -ti:50052 2>/dev/null || true)
        if [ -n "$remaining_pids" ]; then
            echo "Found connector still running on port 50052, cleaning up..."
            kill -9 $remaining_pids 2>/dev/null || true
            sleep 1
        fi

        # Clean up raw log file
        rm -f "$CURRENT_CONNECTOR_LOG.raw" 2>/dev/null || true
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
        # For Docker mode, dump the connector logs with timestamps
        docker logs connector-test 2>&1 | while IFS= read -r line; do echo "[$(date '+%Y-%m-%d %H:%M:%S')] $line"; done > "${LOG_DIR}/${case_name}_connector.log"
    else
        # For non-Docker mode, stop the timestamp process to ensure all logs are flushed
        if [ -n "$TIMESTAMP_PID" ] && [ "$TIMESTAMP_PID" != "0" ]; then
            kill $TIMESTAMP_PID 2>/dev/null || true
            sleep 0.5  # Brief pause to ensure final logs are written
        fi

        # Append any remaining lines from raw log
        if [ -f "$CURRENT_CONNECTOR_LOG.raw" ]; then
            # Get any lines not yet processed
            tail -n +1 "$CURRENT_CONNECTOR_LOG.raw" 2>/dev/null | while IFS= read -r line; do 
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] $line"
            done >> "$CURRENT_CONNECTOR_LOG" 2>/dev/null || true
        fi

        # Ensure the file exists
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

    docker run --mount type=bind,source="$(pwd)/destination-data",target="/data" \
      -a STDIN -a STDOUT -a STDERR $DOCKER_TTY_FLAG \
      $DOCKER_LINK_ARG \
      -e WORKING_DIR="${CONNECTOR_WORKING_DIR}" \
      -e GRPC_HOSTNAME=$GRPC_HOSTNAME \
      us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$SDK_TESTER_TAG \
      --tester-type destination --port 50052 --input-file "input_${case_name}.json" \
      2>&1 | while IFS= read -r line; do echo "[$(date '+%Y-%m-%d %H:%M:%S')] $line"; done | tee "$CURRENT_TESTER_LOG"

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
    ./bin/db-validator "$case_dir/expected.yaml" 2>&1 | while IFS= read -r line; do echo "[$(date '+%Y-%m-%d %H:%M:%S')] $line"; done | tee "$CURRENT_VALIDATOR_LOG"
    validation_result=${PIPESTATUS[0]}

    # Wait for final metrics to be logged
    if [ "$ANALYZE_METRICS" = "true" ]; then
        # Extract the interval duration from METRICS_LOG_INTERVAL
        # Convert to seconds for sleep command
        interval_seconds=5
        if [[ "$METRICS_LOG_INTERVAL" =~ ^([0-9]+)s$ ]]; then
            interval_seconds="${BASH_REMATCH[1]}"
        elif [[ "$METRICS_LOG_INTERVAL" =~ ^([0-9]+)m$ ]]; then
            interval_seconds=$((${BASH_REMATCH[1]} * 60))
        fi

        echo "Waiting ${interval_seconds}s for final metrics to be logged..."
        sleep "$interval_seconds"
    fi

    # Always dump logs regardless of validation result
    echo "Saving test logs..."
    dump_connector_logs "$case_name"

    # Analyze metrics if enabled
    if [ "$ANALYZE_METRICS" = "true" ]; then
        echo "Analyzing connector metrics..."
        analyze_metrics "${LOG_DIR}/${case_name}_connector.log" "$case_name"
    fi

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

    if [ "$ANALYZE_METRICS" = "true" ]; then
        echo ""
        echo "Metrics analysis files:"
        for test in "${passed_tests[@]}" "${failed_tests[@]}"; do
            metrics_file="${LOG_DIR}/${test}_metrics_analysis.txt"
            if [ -f "$metrics_file" ]; then
                echo "  - $metrics_file"
            fi
        done
    fi

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