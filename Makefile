SHELL := /bin/bash

export RUST_BACKTRACE ?= 1
export WASMTIME_BACKTRACE_DETAILS ?= 1
export WKG_CONFIG_FILE := $(abspath .config/wasm-pkg/config.toml)
WASMTIME_RUN_FLAGS ?= -Sinherit-network -Sallow-ip-name-lookup

# `make` without a goal runs all, otherwise the first target of an included file would run
.DEFAULT_GOAL := all

# tools first, the others use the variables it defines for each tool when their rules are read
include make/tools.mk
include make/wit.mk
include make/components.mk
include make/publish.mk


.PHONY: all
all: tools wit components test

.PHONY: clean
clean: clean-components clean-wit 
	@:

.PHONY: clean-all
clean-all: clean-components clean-tools clean-wit
	cargo clean

# runs the cli with the given arguments, e.g. `$(call run_cli,hello)`
run_cli = $(WASMTIME) run $(WASMTIME_RUN_FLAGS) ${COMPONENTS_DIR}/cli/cli.debug.wasm $(1)

.PHONY: run ## Run the cli against a Valkey server, e.g. `make run cmd="keys '*'"`
run: ${COMPONENTS_DIR}/cli/cli.debug.wasm | $(call tool,wasmtime-cli)
	@$(call run_cli,$(cmd))

# the tests that need a Valkey server on localhost:6379, ignored tests in `with_valkey` modules,
# other ignored tests stay ignored
.PHONY: test-valkey
test-valkey: components
	cargo test --workspace -- --ignored with_valkey::
	@$(call run_cli,hello)

.PHONY: test
test: components
	cargo test --workspace
