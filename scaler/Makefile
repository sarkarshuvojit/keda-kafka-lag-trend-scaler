IMAGE_NAME := lag-trend-scaler
IMAGE_TAG := latest

.PHONY: proto-gen
proto-gen:
	protoc --go_out=pkg/externalscaler --go_opt=paths=source_relative \
		--go-grpc_out=pkg/externalscaler --go-grpc_opt=paths=source_relative \
		--proto_path=proto \
		externalscaler.proto

.PHONY: build
build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

# Unit tests only: no external services needed.
.PHONY: test
test:
	go test -race ./...

# Unit + integration tests against a real broker (what CI runs).
# Start one with: docker run -d --rm -p 9092:9092 apache/kafka:3.9.0
KAFKA_BROKERS ?= localhost:9092
COVER_PKGS = $(shell go list ./pkg/... | grep -v /externalscaler | paste -sd, -)

.PHONY: test-integration
test-integration:
	KAFKA_BROKERS=$(KAFKA_BROKERS) go test -race -tags integration -covermode=atomic \
		-coverpkg=$(COVER_PKGS) -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
