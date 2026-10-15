SHELL := /bin/bash

export RUST_BACKTRACE ?= 1
export WASMTIME_BACKTRACE_DETAILS ?= 1
export WKG_CONFIG_FILE := $(abspath .config/wasm-pkg/config.toml)
# the sockets gates of the `cli` need `implements` and `threading`
WASMTIME_RUN_FLAGS ?= -Sinherit-network -Sallow-ip-name-lookup -Sconfig -Wcomponent-model-map -Wcomponent-model-implements -Wcomponent-model-threading

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

# runs the cli with the given arguments, e.g. `$(call run_cli,ping)`
run_cli = $(WASMTIME) run $(WASMTIME_RUN_FLAGS) ${COMPONENTS_DIR}/cli/cli.debug.wasm $(1)

.PHONY: run ## Run the cli against a Valkey server, e.g. `make run cmd="keys '*'"`
run: ${COMPONENTS_DIR}/cli/cli.debug.wasm | $(call tool,wasmtime-cli)
	@$(call run_cli,$(cmd))

# the Valkey server of `make test-valkey`, a container removed afterwards, e.g. `VALKEY_VERSION=8.1`
VALKEY_IMAGE ?= valkey/valkey
VALKEY_VERSION ?= 9.1

# the tests that need a Valkey server, ignored tests in `with_valkey` modules, other ignored tests
# stay ignored. The server runs in a new container, on a free port, so it starts empty.
.PHONY: test-valkey ## Run the tests against a Valkey server in a container, `VALKEY_VERSION` picks the version
test-valkey: components | $(call tool,wasmtime-cli)
	@set -e; \
	container=$$(docker run --detach --rm --publish 127.0.0.1::6379 $(VALKEY_IMAGE):$(VALKEY_VERSION)); \
	trap 'docker stop $$container > /dev/null' EXIT; \
	port=$$(docker port $$container 6379/tcp | head -n 1 | sed 's/.*://'); \
	until docker exec $$container valkey-cli ping > /dev/null 2>&1; do sleep 0.1; done; \
	echo "Valkey $$(docker exec $$container valkey-server --version | sed 's/.* v=\([^ ]*\).*/\1/') on 127.0.0.1:$$port"; \
	VALKEY_HOST=127.0.0.1 VALKEY_PORT=$$port cargo test --workspace --no-fail-fast -- --ignored with_valkey::; \
	$(WASMTIME) run $(WASMTIME_RUN_FLAGS) -Sconfig-var=port=$$port ${COMPONENTS_DIR}/cli/cli.debug.wasm ping; \
	$(WASMTIME) run $(WASMTIME_RUN_FLAGS) -Sconfig-var=hostname=localhost -Sconfig-var=port=$$port ${COMPONENTS_DIR}/cli/cli.debug.wasm ping

.PHONY: test
test: components
	cargo test --workspace
