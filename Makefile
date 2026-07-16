.PHONY: build test clean proto flatbuf install-tools lint vet

# Binary output
BIN_DIR := bin
AION_KERNEL_BIN := $(BIN_DIR)/aion-kernel
ORCHESTRATOR_BIN := $(BIN_DIR)/orchestrator
CLI_BIN := $(BIN_DIR)/orchestrator-cli

# Go settings
GOFLAGS := -v
GOTEST := go test $(GOFLAGS)

# ============================================================
# Build
# ============================================================

build: build-orchestrator build-cli

build-orchestrator:
	@mkdir -p $(BIN_DIR)
	go build $(GOFLAGS) -o $(AION_KERNEL_BIN) ./cmd/orchestrator/
	cp $(AION_KERNEL_BIN) $(ORCHESTRATOR_BIN)

build-cli:
	@mkdir -p $(BIN_DIR)
	go build $(GOFLAGS) -o $(CLI_BIN) ./cmd/orchestrator-cli/

# ============================================================
# Code Generation
# ============================================================

flatbuf:
	@echo "Generating FlatBuffer Go code..."
	flatc --go -o internal/dag/flatbuf/ flatbuf/dag.fbs

proto:
	@echo "Generating protobuf Go code..."
	protoc --go_out=. --go-grpc_out=. proto/*.proto

# ============================================================
# Test
# ============================================================

test:
	$(GOTEST) ./...

test-unit:
	$(GOTEST) -short ./...

test-integration:
	$(GOTEST) -run Integration ./...

test-coverage:
	$(GOTEST) -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# ============================================================
# Quality
# ============================================================

vet:
	go vet ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, running go vet only"; \
		go vet ./...; \
	fi

# ============================================================
# Tools
# ============================================================

install-tools:
	@echo "Installing flatc..."
	@if ! command -v flatc >/dev/null 2>&1; then \
		echo "  Downloading FlatBuffers compiler..."; \
		FLATC_VERSION=24.3.25; \
		curl -sL "https://github.com/google/flatbuffers/releases/download/v$${FLATC_VERSION}/Linux.flatc.binary.g++-13.zip" -o /tmp/flatc.zip; \
		unzip -o /tmp/flatc.zip -d /tmp/flatc; \
		sudo mv /tmp/flatc/flatc /usr/local/bin/flatc; \
		sudo chmod +x /usr/local/bin/flatc; \
		rm -rf /tmp/flatc /tmp/flatc.zip; \
		echo "  flatc installed at $$(which flatc)"; \
	else \
		echo "  flatc already installed: $$(flatc --version)"; \
	fi
	@echo "Installing protoc Go plugins..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Tools installed."

# ============================================================
# Clean
# ============================================================

clean:
	rm -rf $(BIN_DIR)
	rm -f coverage.out coverage.html
