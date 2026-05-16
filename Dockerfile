# Build stage
FROM golang:1.26 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build both binaries with CGO enabled (required by go-duckdb)
RUN CGO_ENABLED=1 go build -o /bin/api ./main.go
RUN CGO_ENABLED=1 go build -o /bin/worker ./worker/

# Runtime stage
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bin/api /bin/api
COPY --from=builder /bin/worker /bin/worker
