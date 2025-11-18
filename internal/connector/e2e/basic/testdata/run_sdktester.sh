#!/usr/bin/env bash

WORKING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SDK_TESTER_TAG=${SDK_TESTER_TAG:-2.25.1105.001}

# This is the directory that is told to the connector, not the tester,
# so that the connector can find the input files.
CONNECTOR_WORKING_DIR="$(cd "$dir/../../../../../" && pwd)"

# This is the hostname that the tester uses to connect to the connector via GRPC.
GRPC_HOSTNAME="${GRPC_HOSTNAME:-host.docker.internal}"

echo "Using the following settings:"
echo "  SDK_TESTER_TAG: $SDK_TESTER_TAG"
echo "  WORKING_DIR: $CONNECTOR_WORKING_DIR"
echo "  GRPC_HOSTNAME: $GRPC_HOSTNAME"

docker run --mount type=bind,source="$WORKING_DIR",target="/data" \
      -a STDIN -a STDOUT -a STDERR \
      -e WORKING_DIR="$WORKING_DIR" \
      -e GRPC_HOSTNAME=$GRPC_HOSTNAME \
      --add-host=host.docker.internal:host-gateway \
      us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$SDK_TESTER_TAG \
      --tester-type destination --port 50052 --input-file "input.json"
