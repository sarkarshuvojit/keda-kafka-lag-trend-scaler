# Build stage
# Build on the runner's native platform and cross-compile for the target,
# so multi-arch release builds don't run the Go toolchain under emulation.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o scaler .

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/scaler .

ENV KAFKA_BROKERS=localhost:9092
ENV KAFKA_TOPIC=test-topic
ENV KAFKA_GROUP_ID=sample-consumer-group

EXPOSE 50051
EXPOSE 9090

ENTRYPOINT ["./scaler"]
