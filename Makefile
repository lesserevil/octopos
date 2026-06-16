# OctopOS Makefile

.PHONY: all build test test-unit test-integration test-e2e clean fmt vet lint ebpf-build bash-build deploy

# Variables
GO_VERSION := 1.22
BINARY_NAME := octoposd
BASH_BINARY := octo-bash
BUILD_DIR := ./bin
GO_PKGS := ./cmd/... ./pkg/... ./test/...

# Default target
all: build

# Build Go binaries
build: $(BUILD_DIR)/$(BINARY_NAME)

$(BUILD_DIR)/$(BINARY_NAME):
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/octoposd

# Build octoposctl
$(BUILD_DIR)/octoposctl:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octoposctl ./cmd/octoposctl

# Build all CLI tools
build-tools: $(BUILD_DIR)/$(BINARY_NAME) $(BUILD_DIR)/octoposctl

# Run tests
test: test-unit

test-unit:
	go test -v -race -coverprofile=coverage.out $(GO_PKGS)

test-integration:
	@echo "Running integration tests (requires 3-node cluster)..."
	go test -v -tags=integration ./test/integration/...

test-e2e:
	@echo "Running E2E tests..."
	go test -v -tags=e2e ./test/e2e/...

# Code quality
fmt:
	gofmt -w -s ./cmd ./pkg ./test

vet:
	go vet $(GO_PKGS)

lint:
	golangci-lint run ./...

# Generate gRPC code from proto
generate:
	protoc --go_out=. --go-grpc_out=. --go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		./pkg/rpc/octopos.proto

# eBPF build
ebpf-build:
	@echo "Building eBPF programs..."
	@for dir in ./ebpf/*/; do \
		if [ -f "$$dir/Makefile" ]; then \
			echo "Building $$dir"; \
			$(MAKE) -C "$$dir"; \
		fi; \
	done

ebpf-verify:
	@echo "Verifying eBPF programs with bpftool..."
	@for dir in ./ebpf/*/; do \
		if [ -f "$$dir/*.bpf.o" ]; then \
			bpftool prog load "$$dir/*.bpf.o" /sys/fs/bpf/test type tracepoint 2>&1 | head -5; \
		fi; \
	done

# Bash fork build (requires bash source)
bash-build:
	@echo "Building octo-bash..."
	@if [ -d "./cmd/octo-bash/bash-src" ]; then \
		cd ./cmd/octo-bash/bash-src && ./configure && make; \
	else \
		echo "Bash source not found. Run: git clone --depth 1 -b bash-5.2 https://git.savannah.gnu.org/git/bash.git cmd/octo-bash/bash-src"; \
	fi

# Development
dev: build
	$(BUILD_DIR)/$(BINARY_NAME) -config=/etc/octopos/octoposd.yaml

# Clean
clean:
	rm -rf $(BUILD_DIR)
	go clean -cache -testcache
	@for dir in ./ebpf/*/; do \
		if [ -f "$$dir/Makefile" ]; then \
			$(MAKE) -C "$$dir" clean; \
		fi; \
	done

# Install to system
install: build
	sudo cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/
	sudo mkdir -p /etc/octopos
	sudo cp deploy/systemd/octoposd.service /etc/systemd/system/
	sudo systemctl daemon-reload

# Deploy to test cluster
deploy: build
	@for node in 1 2 3; do \
		IP=$$(./clusterctl.sh ips | grep "node-$$node" | awk '{print $$2}'); \
		echo "Deploying to $$IP..."; \
		scp $(BUILD_DIR)/$(BINARY_NAME) ubuntu@$$IP:/tmp/; \
		ssh ubuntu@$$IP "sudo cp /tmp/$(BINARY_NAME) /usr/local/bin/ && sudo systemctl restart octoposd"; \
	done

# Quick status check
status:
	./clusterctl.sh status

# Help
help:
	@echo "OctopOS Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  build           - Build octoposd binary"
	@echo "  build-tools     - Build all CLI tools"
	@echo "  test            - Run unit tests"
	@echo "  test-unit       - Run unit tests with coverage"
	@echo "  test-integration - Run integration tests"
	@echo "  test-e2e        - Run E2E tests"
	@echo "  fmt             - Format code"
	@echo "  vet             - Run go vet"
	@echo "  lint            - Run golangci-lint"
	@echo "  generate        - Generate gRPC code from proto"
	@echo "  ebpf-build      - Build eBPF programs"
	@echo "  ebpf-verify     - Verify eBPF programs"
	@echo "  bash-build      - Build octo-bash (bash fork)"
	@echo "  clean           - Clean build artifacts"
	@echo "  install         - Install to system"
	@echo "  deploy          - Deploy to test cluster"
	@echo "  status          - Check cluster status"