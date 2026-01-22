# Build stage
FROM golang:alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN go build -o hyperliquid-funding-job ./cmd/funding-job/main.go

# Final stage
FROM alpine:latest

WORKDIR /root/

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Copy the Pre-built binary from the previous stage
COPY --from=builder /app/hyperliquid-funding-job .

# Command to run the executable
CMD ["./hyperliquid-funding-job"]
