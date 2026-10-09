COMPONENTS_DIR := target/components

COMPONENTS = $(sort $(foreach file,$(wildcard $(addprefix components/*/,wit/*.constants.wit *.properties *.wac *.wkg Cargo.toml)),$(word 2,$(subst /, ,$(file)))))

.PHONY: clean-components
clean-components: clean-wit
	rm -rf ${COMPONENTS_DIR}

.PHONY: components
components: ${COMPONENTS_DIR}/interface.wasm $(foreach component,$(COMPONENTS),${COMPONENTS_DIR}/$(component)/$(component).wasm ${COMPONENTS_DIR}/$(component)/$(component).debug.wasm)

define BUILD_COMPONENT

.PHONY: components/$1
components/$1: ${COMPONENTS_DIR}/$1/$1.wasm ${COMPONENTS_DIR}/$1/$1.debug.wasm

ifneq ($(wildcard components/$1/wit/$1.constants.wit),)

${COMPONENTS_DIR}/$1/$1.wasm: components/$1/wit/deps ${COMPONENTS_DIR}/$1/README.md | $(call tool,componentized-constants-cli)
	$(CONSTANTS) --wit components/$1/wit -o ${COMPONENTS_DIR}/$1/$1.wasm

${COMPONENTS_DIR}/$1/$1.debug.wasm: components/$1/wit/deps ${COMPONENTS_DIR}/$1/README.md | $(call tool,componentized-constants-cli)
	$(CONSTANTS) --wit components/$1/wit -o ${COMPONENTS_DIR}/$1/$1.debug.wasm

else ifneq ($(wildcard components/$1/$1.properties),)

${COMPONENTS_DIR}/$1/$1.wasm: components/$1/$1.properties ${COMPONENTS_DIR}/$1/README.md | $(call tool,componentized-static-config-cli)
	$(STATIC_CONFIG) -f components/$1/$1.properties -o ${COMPONENTS_DIR}/$1/$1.wasm

${COMPONENTS_DIR}/$1/$1.debug.wasm: components/$1/$1.properties ${COMPONENTS_DIR}/$1/README.md | $(call tool,componentized-static-config-cli)
	$(STATIC_CONFIG) -f components/$1/$1.properties -o ${COMPONENTS_DIR}/$1/$1.debug.wasm

else ifneq ($(wildcard components/$1/$1.wac),)

# the local packages the composition instantiates, e.g. `new local:latch-n2 { ... }`
WAC_DEPS_$1 := $$(shell grep -v '^\s*//' components/$1/$1.wac | grep -oE 'local:[a-z0-9-]+' | sed 's/^local://' | sort -u)

${COMPONENTS_DIR}/$1/$1.wasm: components/$1/$1.wac $$(foreach component,$$(WAC_DEPS_$1),$${COMPONENTS_DIR}/$$(component)/$$(component).wasm) ${COMPONENTS_DIR}/$1/README.md | $(call tool,wac-cli)
	$(WAC) compose $$(foreach component,$$(WAC_DEPS_$1),-d local:$$(component)=$${COMPONENTS_DIR}/$$(component)/$$(component).wasm) -o ${COMPONENTS_DIR}/$1/$1.wasm components/$1/$1.wac

${COMPONENTS_DIR}/$1/$1.debug.wasm: components/$1/$1.wac $$(foreach component,$$(WAC_DEPS_$1),$${COMPONENTS_DIR}/$$(component)/$$(component).debug.wasm) ${COMPONENTS_DIR}/$1/README.md | $(call tool,wac-cli)
	$(WAC) compose $$(foreach component,$$(WAC_DEPS_$1),-d local:$$(component)=$${COMPONENTS_DIR}/$$(component)/$$(component).debug.wasm) -o ${COMPONENTS_DIR}/$1/$1.debug.wasm components/$1/$1.wac

else ifneq ($(wildcard components/$1/$1.wkg),)

${COMPONENTS_DIR}/$1/$1.wasm: components/$1/$1.wkg ${COMPONENTS_DIR}/$1/README.md | $(call tool,wkg)
	$(WKG) oci pull $(shell cat components/$1/$1.wkg 2> /dev/null | head -1) -o ${COMPONENTS_DIR}/$1/$1.wasm

${COMPONENTS_DIR}/$1/$1.debug.wasm: components/$1/$1.wkg ${COMPONENTS_DIR}/$1/README.md | $(call tool,wkg)
	$(WKG) oci pull $(shell cat components/$1/$1.wkg  2> /dev/null | tail -1 2> /dev/null) -o ${COMPONENTS_DIR}/$1/$1.debug.wasm

# cargo is checked last, other strategies may have a Cargo.toml for tests of non-rust sources
else ifneq ($(wildcard components/$1/Cargo.toml),)

${COMPONENTS_DIR}/$1/$1.wasm: Cargo.toml Cargo.lock components/wit/deps $(shell find components/$1 -type f) $(shell find crates -type f 2> /dev/null) ${COMPONENTS_DIR}/$1/README.md | $(call tool,wasm-tools)
	cargo build -p $1 --target wasm32-unknown-unknown --release
	$(WASM_TOOLS) component new target/wasm32-unknown-unknown/release/$(subst -,_,$1).wasm -o ${COMPONENTS_DIR}/$1/$1.wasm

${COMPONENTS_DIR}/$1/$1.debug.wasm: Cargo.toml Cargo.lock components/wit/deps $(shell find components/$1 -type f) $(shell find crates -type f 2> /dev/null) ${COMPONENTS_DIR}/$1/README.md | $(call tool,wasm-tools)
	cargo build --target wasm32-unknown-unknown -p $1
	$(WASM_TOOLS) component new target/wasm32-unknown-unknown/debug/$(subst -,_,$1).wasm -o ${COMPONENTS_DIR}/$1/$1.debug.wasm

endif

${COMPONENTS_DIR}/$1/README.md: components/$1/README.md
	@mkdir -p ${COMPONENTS_DIR}/$1
	@cp components/$1/README.md ${COMPONENTS_DIR}/$1/README.md

endef

$(foreach component,$(COMPONENTS),$(eval $(call BUILD_COMPONENT,$(component))))

${COMPONENTS_DIR}/interface.wasm: wit/deps README.md | $(call tool,wkg)
	@mkdir -p ${COMPONENTS_DIR}
	$(WKG) build -o ${COMPONENTS_DIR}/interface.wasm
	@cp README.md ${COMPONENTS_DIR}/README.md
