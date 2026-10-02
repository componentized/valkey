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
components: lib/interface.wasm lib/cli.wasm lib/cli.debug.wasm lib/as-keyvalue.wasm lib/as-keyvalue.debug.wasm lib/client.wasm lib/client.debug.wasm lib/ops.wasm lib/ops.debug.wasm lib/sample-http-incrementor.wasm lib/sample-http-incrementor.debug.wasm

lib/interface.wasm: wit/deps README.md
	wkg build -o lib/interface.wasm
	cp README.md lib/interface.wasm.md

define BUILD_COMPONENT
# $1 - component
# $2 - target
# $3 - release target deps
# $4 - debug target deps

lib/$1.wasm: $3 Cargo.toml Cargo.lock components/wit/deps $(shell find components/wit -type f) $(shell find components/$1 -type f)
	cargo build -p $1 --target $2 --release
	$(if $(findstring $1,cli),
		wac plug target/$2/release/$(subst -,_,$1).wasm --plug lib/ops.wasm -o lib/$1.wasm,
		wasm-tools component new target/$2/release/$(subst -,_,$1).wasm -o lib/$1.wasm)
	cp components/$1/README.md lib/$1.wasm.md

lib/$1.debug.wasm: $4 Cargo.toml Cargo.lock wit/deps $(shell find components/$1 -type f)
	cargo build -p $1 --target $2
	$(if $(findstring $1,cli),
		wac plug target/$2/debug/$(subst -,_,$1).wasm --plug lib/ops.debug.wasm -o lib/$1.debug.wasm,
		wasm-tools component new target/$2/debug/$(subst -,_,$1).wasm -o lib/$1.debug.wasm)
	cp components/$1/README.md lib/$1.debug.wasm.md

endef

$(eval $(call BUILD_COMPONENT,ops,wasm32-unknown-unknown))
$(eval $(call BUILD_COMPONENT,as-keyvalue,wasm32-unknown-unknown))
$(eval $(call BUILD_COMPONENT,cli,wasm32-wasip2,lib/ops.wasm,lib/ops.debug.wasm))
$(eval $(call BUILD_COMPONENT,sample-http-incrementor,wasm32-unknown-unknown))

lib/client.wasm: components/client.wac lib/ops.wasm lib/as-keyvalue.wasm
	wac compose -o lib/client.wasm \
		-d local:ops=./lib/ops.wasm \
		-d local:as-keyvalue=./lib/as-keyvalue.wasm \
		components/client.wac
	cp README.md lib/client.wasm.md

lib/client.debug.wasm: components/client.wac lib/ops.debug.wasm lib/as-keyvalue.debug.wasm
	wac compose -o lib/client.debug.wasm \
		-d local:ops=./lib/ops.debug.wasm \
		-d local:as-keyvalue=./lib/as-keyvalue.debug.wasm \
		components/client.wac
	cp README.md lib/client.debug.wasm.md

.PHONY: wit
wit: wit/deps components/wit/deps

wit/deps: wkg.toml $(shell find wit -type f -name "*.wit" -not -path "deps")
	wkg fetch

components/wit/deps: wit/deps components/wkg.toml $(shell find components/wit -type f -name "*.wit" -not -path "deps")
	(cd components && wkg fetch)

.PHONY: test
test:
	@$(MAKE) run cmd="hello"

.PHONY: publish ## Publish each component in the lib directory
publish: $(shell find lib -maxdepth 1 -type f -name "*.wasm" | sed -e 's:^lib/:publish-:g')

.PHONY: publish-%
publish-%:
ifndef VERSION
	$(error VERSION is undefined)
endif
ifndef REPOSITORY
	$(error REPOSITORY is undefined)
endif
	@$(eval FILE := $(@:publish-%=%))
	@$(eval COMPONENT := $(if $(filter %.debug.wasm,$(FILE)),$(FILE:%.debug.wasm=%),$(FILE:%.wasm=%)))
	@$(eval TITLE := $(subst /,:,$(GITHUB_REPOSITORY))$(if $(filter interface,$(COMPONENT)),,-$(COMPONENT))$(if $(filter %.debug.wasm,$(FILE)), (debug)))
	@$(eval DESCRIPTION := $(shell head -n 3 "lib/${FILE}.md" | tail -n 1))
	@$(eval COMMIT := $(shell git rev-parse HEAD))
	@$(eval README_DIR := $(if $(wildcard components/$(COMPONENT)/README.md),/components/$(COMPONENT)))
	@$(eval URL := https://github.com/${GITHUB_REPOSITORY}/tree/${COMMIT}${README_DIR})
	@$(eval REVISION := ${COMMIT}$(shell git diff --quiet HEAD || echo "+dirty"))
	@$(eval COMPONENT_VERSION := $(if $(filter %.debug.wasm,$(FILE)),${VERSION}+debug,${VERSION}))
	@$(eval TAG := $(patsubst v%,%,$(subst +,_,$(COMPONENT_VERSION))))
	@$(eval IMAGE := $(if $(filter interface.wasm,$(FILE)),${REPOSITORY}:${TAG},${REPOSITORY}/${COMPONENT}:${TAG}))

	@echo "::group::${FILE} -> ${IMAGE}"
	@DIGEST=$$( \
		wkg oci push \
			--annotation "org.opencontainers.image.title=${TITLE}" \
			--annotation "org.opencontainers.image.description=${DESCRIPTION}" \
			--annotation "org.opencontainers.image.version=${COMPONENT_VERSION}" \
			--annotation "org.opencontainers.image.url=${URL}" \
			--annotation "org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY}.git" \
			--annotation "org.opencontainers.image.revision=${REVISION}" \
			--annotation "org.opencontainers.image.licenses=Apache-2.0" \
			"${IMAGE}" \
			"lib/${FILE}" \
			2>&1 \
			| tee /dev/stderr \
			| grep -o 'sha256:[a-f0-9]\{64\}' \
	) ; \
	cosign sign --yes "${IMAGE}@$${DIGEST}"
	@echo "::endgroup::"
