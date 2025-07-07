.PHONY: all build test clean generate download-protos test-destination lint fmt

# Allow injection of additional go test arguments
# Examples:
# - GOTEST_ARGS="-memprofile=profiles/test_mem.prof" ./internal/connector)
# - GOTEST_ARGS="-run TestMemory ./..."
# - GOTEST_ARGS="-race ./..."
# - GOTEST_ARGS="-bench=BenchmarkMemoryAllocation -benchmem ./internal/connector"
GOTEST_ARGS ?= ./...

all: generate build test test-destination lint

download-protos:
	mkdir -p proto
	curl -o proto/destination_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/main/destination_sdk.proto
	curl -o proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/main/common.proto

generate: download-protos
	mkdir -p internal/pb
	protoc -I=proto \
		--go_out=internal/pb --go_opt=paths=source_relative \
		--go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
		proto/*.proto

build:
	go build -o bin/connector

test:
	go test $(GOTEST_ARGS)

test-destination:
	@echo "Running destination connector conformance tests..."
	@cd tests && ./destination-connector-test.sh

lint:
	@echo "Running golangci-lint..."
	@docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:v2.1.1 golangci-lint run --timeout=5m

fmt:
	@echo "Formatting Go code..."
	@go fmt ./...
	@echo "Go code formatted successfully"

clean:
	rm -rf bin/
	rm -rf proto/
	rm -rf internal/pb/
