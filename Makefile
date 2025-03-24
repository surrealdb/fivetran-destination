.PHONY: all build test clean generate download-protos

all: generate build test

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
	go test ./...

clean:
	rm -rf bin/
	rm -rf proto/
	rm -rf internal/pb/ 