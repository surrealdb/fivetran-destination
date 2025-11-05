FROM mcr.microsoft.com/devcontainers/go:1.25-bookworm

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV GO111MODULE=on
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:$PATH

WORKDIR /workspace
