FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o bin/connector

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/bin/connector /app/connector

EXPOSE 50052

CMD ["/app/connector", "--port", "50052"]
