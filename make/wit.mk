# a path relative to the root of the repository, e.g. `wit` for `components/../wit`
relpath = $(if $(filter $(CURDIR),$(abspath $(1))),.,$(patsubst $(CURDIR)/%,%,$(abspath $(1))))

# directories with a wkg.toml, each fetches the dependencies of its wit directory into wit/deps,
# e.g. `.` and `components`
WKG_DIRS := $(sort $(patsubst ./%,%,$(patsubst %/,%,$(dir $(shell find . -name wkg.toml -not -path './target/*' -not -path '*/deps/*')))))

# the wit/deps directory of a directory with a wkg.toml, e.g. `wit/deps` for `.`
wit_deps = $(patsubst ./%,%,$(1)/wit/deps)

WIT_DEPS := $(foreach dir,$(WKG_DIRS),$(call wit_deps,$(dir)))

.PHONY: clean-wit ## Remove the fetched wit dependencies, fetched again by `make wit`
clean-wit:
	rm -rf $(WIT_DEPS)

.PHONY: wit
wit: $(WIT_DEPS)

define FETCH_WIT

# a package overridden with a local path, e.g. `{ path = "../wit" }`, has its dependencies fetched first,
# an override without dependencies of its own is skipped, e.g. `{ path = "./wit/overrides/wasi-keyvalue" }`
$(call wit_deps,$1): $1/wkg.toml $1/wkg.lock $(shell find $1/wit -type f -name "*.wit" -not -path "*/deps/*") $(filter $(WIT_DEPS),$(foreach path,$(shell sed -n 's/.*path *= *"\(.*\)".*/\1/p' $1/wkg.toml),$(call relpath,$1/$(path))/deps)) | $(call tool,wkg)
	$(if $(filter .,$1),,cd $1 && )$(WKG) fetch

endef

$(foreach dir,$(WKG_DIRS),$(eval $(call FETCH_WIT,$(dir))))
