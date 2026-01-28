FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o bin/server

FROM alpine:3.23.3

WORKDIR /app

COPY --from=builder /app/bin/server /app/server

EXPOSE 50052

CMD ["/app/server", "--port", "50052"]
