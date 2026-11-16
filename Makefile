# protag monorepo root targets.
#
# The kernel keeps its own build/test targets in kernel/Makefile; the root
# delegates so the whole workspace can be exercised with one command.

SHELL := /bin/bash
GOTEST ?= go test
export GOCACHE ?= /tmp/aion-go-cache

.PHONY: help build test test-core test-scenarios test-scenarios-shard test-scenarios-process test-scenarios-race test-scenarios-live test-scenarios-flake fmt vet clean

help:
	@echo "protag monorepo targets"
	@echo "  make build                  build kernel binaries"
	@echo "  make test                   test every module in the workspace"
	@echo "  make test-core              kernel unit tests only"
	@echo "  make test-scenarios         deterministic scenario matrix"
	@echo "  make test-scenarios-race    deterministic scenarios under -race"
	@echo "  make test-scenarios-shard   one shard (SHARD=n SHARDS=m)"
	@echo "  make test-scenarios-process process-level scenarios"
	@echo "  make test-scenarios-live    opt-in live model scenarios"
	@echo "  make test-scenarios-flake   repeated runs with flake history"
	@echo "  make fmt / make vet         workspace formatting and vet"

build:
	$(MAKE) -C kernel build

test: test-core
	cd coordination && $(GOTEST) ./... -count=1
	cd isolation && $(GOTEST) ./... -count=1

test-core:
	cd kernel && $(GOTEST) ./... -count=1

test-scenarios:
	$(MAKE) -C kernel test-scenarios

test-scenarios-shard:
	$(MAKE) -C kernel test-scenarios-shard

test-scenarios-process:
	$(MAKE) -C kernel test-scenarios-process

test-scenarios-race:
	$(MAKE) -C kernel test-scenarios-race

test-scenarios-live:
	$(MAKE) -C kernel test-scenarios-live

test-scenarios-flake:
	$(MAKE) -C kernel test-scenarios-flake

fmt:
	gofmt -w coordination isolation kernel

vet:
	cd coordination && go vet ./...
	cd isolation && go vet ./...
	cd kernel && go vet ./...

clean:
	$(MAKE) -C kernel clean
