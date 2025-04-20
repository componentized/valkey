export RUST_BACKTRACE ?= 1
export WASMTIME_BACKTRACE_DETAILS ?= 1
WASMTIME_RUN_FLAGS ?= -Sinherit-network -Sallow-ip-name-lookup

.PHONY: all
all: components

.PHONY: clean
clean:
	cargo clean
	rm -rf lib/*.wasm
	rm -rf lib/*.wasm.md

.PHONY: run
run: lib/cli.debug.wasm
	@wasmtime run $(WASMTIME_RUN_FLAGS) lib/cli.debug.wasm $(cmd)

.PHONY: components
components: lib/interface.wasm lib/cli.wasm lib/cli.debug.wasm lib/keyvalue-to-valkey.wasm lib/keyvalue-to-valkey.debug.wasm lib/valkey-client.wasm lib/valkey-client.debug.wasm lib/valkey-ops.wasm lib/valkey-ops.debug.wasm

lib/interface.wasm: wit/deps README.md
	wkg wit build -o lib/interface.wasm
	cp README.md lib/interface.wasm.md

define BUILD_COMPONENT
# $1 - component
# $2 - release target
# $3 - rust toolchain
# $4 - release target deps
# $5 - debug target deps

lib/$1.wasm: $4 Cargo.toml Cargo.lock components/wit/deps $(shell find components/wit -type f) $(shell find components/$1 -type f)
	cargo $3 component build -p $1 --target $2 --release
	$(if $(findstring $1,cli),
		wac plug target/$2/release/$(subst -,_,$1).wasm --plug lib/valkey-client.wasm -o lib/$1.wasm,
		cp target/$2/release/$(subst -,_,$1).wasm lib/$1.wasm)
	cp components/$1/README.md lib/$1.wasm.md

lib/$1.debug.wasm: $5 Cargo.toml Cargo.lock wit/deps $(shell find components/$1 -type f)
	cargo +nightly component build -p $1 --target wasm32-wasip2
	$(if $(findstring $1,cli),
		wac plug target/$2/debug/$(subst -,_,$1).wasm --plug lib/valkey-client.debug.wasm -o lib/$1.debug.wasm,
		cp target/wasm32-wasip2/debug/$(subst -,_,$1).wasm lib/$1.debug.wasm)
	cp components/$1/README.md lib/$1.debug.wasm.md

endef

$(eval $(call BUILD_COMPONENT,valkey-ops,wasm32-unknown-unknown))
$(eval $(call BUILD_COMPONENT,keyvalue-to-valkey,wasm32-unknown-unknown))
$(eval $(call BUILD_COMPONENT,cli,wasm32-wasip2,+nightly,lib/valkey-client.wasm,lib/valkey-client.debug.wasm))

lib/valkey-client.wasm: components/valkey-client.wac lib/valkey-ops.wasm lib/keyvalue-to-valkey.wasm
	wac compose -o lib/valkey-client.wasm \
		-d componentized:valkey-ops=./lib/valkey-ops.wasm \
		-d componentized:keyvalue-to-valkey=./lib/keyvalue-to-valkey.wasm \
		components/valkey-client.wac
	cp README.md lib/valkey-client.wasm.md

lib/valkey-client.debug.wasm: components/valkey-client.wac lib/valkey-ops.debug.wasm lib/keyvalue-to-valkey.debug.wasm
	wac compose -o lib/valkey-client.debug.wasm \
		-d componentized:valkey-ops=./lib/valkey-ops.debug.wasm \
		-d componentized:keyvalue-to-valkey=./lib/keyvalue-to-valkey.debug.wasm \
		components/valkey-client.wac
	cp README.md lib/valkey-client.debug.wasm.md

.PHONY: wit
wit: wit/deps components/wit/deps

wit/deps: wkg.toml $(shell find wit -type f -name "*.wit" -not -path "deps")
	wkg wit fetch

components/wit/deps: wit/deps components/wkg.toml $(shell find components/wit -type f -name "*.wit" -not -path "deps")
	(cd components && wkg wit fetch)

.PHONY: publish
publish: $(shell find lib -type f -name "*.wasm" | sed -e 's:^lib/:publish-:g')

.PHONY: publish-%
publish-%:
ifndef VERSION
	$(error VERSION is undefined)
endif
ifndef REPOSITORY
	$(error REPOSITORY is undefined)
endif
	@$(eval FILE := $(@:publish-%=%))
	@$(eval COMPONENT := $(FILE:%.wasm=%))
	@$(eval DESCRIPTION := $(shell head -n 3 "lib/${FILE}.md" | tail -n 1))
	@$(eval REVISION := $(shell git rev-parse HEAD)$(shell git diff --quiet HEAD && echo "+dirty"))
	@$(eval TAG := $(shell echo "${VERSION}" | sed 's/[^a-zA-Z0-9_.\-]/--/g'))

	wkg oci push \
        --annotation "org.opencontainers.image.title=${COMPONENT}" \
        --annotation "org.opencontainers.image.description=${DESCRIPTION}" \
        --annotation "org.opencontainers.image.version=${VERSION}" \
        --annotation "org.opencontainers.image.source=https://github.com/componentized/valkey.git" \
        --annotation "org.opencontainers.image.revision=${REVISION}" \
        --annotation "org.opencontainers.image.licenses=Apache-2.0" \
        "${REPOSITORY}/${COMPONENT}:${TAG}" \
        "lib/${FILE}"
