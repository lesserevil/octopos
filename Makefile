# OctopOS Makefile

.PHONY: all build build-daemon build-octoposd build-tools build-octoposctl build-octopos-exec build-octopos-remote-child build-octopos-child-supervisor build-remotechild-preload build-octopos-gw build-octopos-objectstore-proxy build-fuse build-octopos-procfs build-octopos-devfs build-octopos-sysfs test test-unit test-tools test-integration test-e2e clean fmt vet lint generate ebpf-build ebpf-verify bash-build dev install deploy status help

# Variables
GO_VERSION := 1.22
BINARY_NAME := octoposd
BASH_BINARY := octo-bash
BUILD_DIR := ./bin
CC ?= cc
GO_BINARIES := octoposd octoposctl octopos-exec octopos-remote-child octopos-child-supervisor octopos-gw octopos-objectstore-proxy octopos-procfs octopos-devfs octopos-sysfs
TOOL_PKGS := ./cmd/octoposctl ./cmd/octopos-exec ./cmd/octopos-remote-child ./cmd/octopos-child-supervisor ./cmd/octopos-gw ./cmd/octopos-objectstore-proxy ./fuse/procfs ./fuse/devfs ./fuse/sysfs
GO_PKGS := ./cmd/... ./pkg/... ./fuse/...
REMOTECHILD_PRELOAD := $(BUILD_DIR)/liboctopos_remotechild_preload.so

# Default target
all: build

# Build all Go binaries
build: build-daemon build-tools

build-daemon: build-octoposd

build-octoposd:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/octoposd

# Build support tools
build-tools: build-octoposctl build-octopos-exec build-octopos-remote-child build-octopos-child-supervisor build-remotechild-preload build-octopos-gw build-octopos-objectstore-proxy build-fuse

build-octoposctl:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octoposctl ./cmd/octoposctl

build-octopos-exec:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octopos-exec ./cmd/octopos-exec

build-octopos-remote-child:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octopos-remote-child ./cmd/octopos-remote-child

build-octopos-child-supervisor:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octopos-child-supervisor ./cmd/octopos-child-supervisor

build-remotechild-preload:
	@mkdir -p $(BUILD_DIR)
	$(CC) -shared -fPIC -O2 -Wall -Wextra -o $(REMOTECHILD_PRELOAD) runtime/remotechild-preload/remotechild_preload.c -ldl

build-octopos-gw:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octopos-gw ./cmd/octopos-gw

build-octopos-objectstore-proxy:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octopos-objectstore-proxy ./cmd/octopos-objectstore-proxy

build-fuse: build-octopos-procfs build-octopos-devfs build-octopos-sysfs

build-octopos-procfs:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octopos-procfs ./fuse/procfs

build-octopos-devfs:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octopos-devfs ./fuse/devfs

build-octopos-sysfs:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/octopos-sysfs ./fuse/sysfs

# Run tests
test: test-unit

test-unit:
	go test -v -race -coverprofile=coverage.out $(GO_PKGS)

test-tools:
	go test -v $(TOOL_PKGS)

test-integration:
	@echo "Running integration tests (requires 3-node cluster)..."
	go test -v -tags=integration ./test/integration/...

test-e2e:
	@echo "Running E2E tests..."
	go test -v -tags=e2e ./test/e2e/...

# Code quality
fmt:
	gofmt -w -s ./cmd ./pkg ./fuse

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
	sudo install -d /usr/local/bin /usr/local/lib/octopos /etc/octopos /etc/systemd/system
	sudo install -m 0755 $(addprefix $(BUILD_DIR)/,$(GO_BINARIES)) /usr/local/bin/
	sudo install -m 0755 $(REMOTECHILD_PRELOAD) /usr/local/lib/octopos/
	sudo install -m 0644 deploy/systemd/octoposd.service deploy/systemd/octopos-gw.service /etc/systemd/system/
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
	@echo "  build           - Build all Go binaries"
	@echo "  build-daemon    - Build octoposd binary"
	@echo "  build-tools     - Build CLI, gateway, and FUSE tools"
	@echo "  build-fuse      - Build FUSE filesystem daemons"
	@echo "  test            - Run unit tests"
	@echo "  test-unit       - Run unit tests with coverage"
	@echo "  test-tools      - Run executable package tests"
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
