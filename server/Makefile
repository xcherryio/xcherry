# get rid of default behaviors, they're just noise
MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

# Monorepo root go.work intentionally excludes this module (Cadence/Temporal
# genproto conflict). Force module mode for all Make targets.
export GOWORK := off

default: help

# ###########################################
#                TL;DR DOCS:
# ###########################################
# - Targets should never, EVER be *actual source files*.
#   Always use book-keeping files in $(BUILD).
#   Otherwise e.g. changing git branches could confuse Make about what it needs to do.
# - Similarly, prerequisites should be those book-keeping files,
#   not source files that are prerequisites for book-keeping.
#   e.g. depend on .build/fmt, not $(ALL_SRC), and not both.
# - Be strict and explicit about prerequisites / order of execution / etc.
# - Test your changes with `-j 27 --output-sync` or something!
# - Test your changes with `make -d ...`!  It should be reasonable!

# temporary build products and book-keeping targets that are always good to / safe to clean.
BUILD := .build
# less-than-temporary build products, e.g. tools.
# usually unnecessary to clean, and may require downloads to restore, so this folder is not automatically cleaned.
BIN := .bin

# ====================================
# book-keeping files that are used to control sequencing.
#
# you should use these as prerequisites in almost all cases, not the source files themselves.
# these are defined in roughly the reverse order that they are executed, for easier reading.
#
# recipes and any other prerequisites are defined only once, further below.
# ====================================

# ====================================
# helper vars
# ====================================

# set a VERBOSE=1 env var for verbose output. VERBOSE=0 (or unset) disables.
# this is used to make verbose flags, suitable for `$(if $(test_v),...)`.
VERBOSE ?= 0
ifneq (0,$(VERBOSE))
test_v = 1
else
test_v =
endif

# a literal space value, for makefile purposes
SPACE :=
SPACE +=
COMMA := ,

# M1 macs may need to switch back to x86, until arm releases are available
EMULATE_X86 =
ifeq ($(shell uname -sm),Darwin arm64)
EMULATE_X86 = arch -x86_64
endif

# helper for executing bins that need other bins, just `$(BIN_PATH) the_command ...`
# I'd recommend not exporting this in general, to reduce the chance of accidentally using non-versioned tools.
BIN_PATH := PATH="$(abspath $(BIN)):$$PATH"

# version, git sha, etc flags.
# reasonable to make a :=, but it's only used in one place, so just leave it lazy or do it inline.
GO_BUILD_LDFLAGS = $(shell ./scripts/go-build-ldflags.sh LDFLAG)

# automatically gather all source files that currently exist.
# works by ignoring everything in the parens (and does not descend into matching folders) due to `-prune`,
# and everything else goes to the other side of the `-o` branch, which is `-print`ed.
# this is dramatically faster than a `find . | grep -v vendor` pipeline, and scales far better.
FRESH_ALL_SRC = $(shell \
	find . \
	\( \
		-path './vendor/*' \
		-o -path './idls/*' \
		-o -path './.build/*' \
		-o -path './.bin/*' \
	\) \
	-prune \
	-o -name '*.go' -print \
)
# most things can use a cached copy, e.g. all dependencies.
# this will not include any files that are created during a `make` run, e.g. via protoc,
# but that generally should not matter (e.g. dependencies are computed at parse time, so it
# won't affect behavior either way - choose the fast option).
#
# if you require a fully up-to-date list, e.g. for shell commands, use FRESH_ALL_SRC instead.
ALL_SRC := $(FRESH_ALL_SRC)
# as lint ignores generated code, it can use the cached copy in all cases
LINT_SRC := $(filter-out %_test.go ./.gen/%, $(ALL_SRC))

# ====================================
# $(BIN) targets
# ====================================

# downloads and builds a go-gettable tool, versioned by go.mod, and installs
# it into the build folder, named the same as the last portion of the URL.
define go_build_tool
@echo "building $(notdir $(1)) from $(1)..."
@go build -mod=readonly -o $(BIN)/$(notdir $(1)) $(1)
endef

# ====================================
# developer-oriented targets
#
# many of these share logic with other intermediates, but are useful to make .PHONY for output on demand.
# as the Makefile is fast, it's reasonable to just delete the book-keeping file recursively make.
# this way the effort is shared with future `make` runs.
# ====================================

# "re-make" a target by deleting and re-building book-keeping target(s).
# the + is necessary for parallelism flags to be propagated
define remake
@rm -f $(addprefix $(BUILD)/,$(1))
@+$(MAKE) --no-print-directory $(addprefix $(BUILD)/,$(1))
endef

# useful to actually re-run to get output again.
# reuse the intermediates for simplicity and consistency.
lint: ## (re)run the linter
	$(call remake,proto-lint lint)

# intentionally not re-making, goimports is slow and it's clear when it's unnecessary
fmt: $(BUILD)/fmt ## run goimports

# ====================================
# binaries to build
# ====================================
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

BINS =

BINS += dex-server
dex-server:
	@echo "compiling dex-server with OS: $(GOOS), ARCH: $(GOARCH)"
	@cd ../web && npm ci && npm run build
	@cd cmd/server && go build -o ../../dex-server .

.PHONY: bins release clean replayTests integrationCoverage prepareIntegrationCoverage serverCommandIntegTests streamIntegTests streamBenchmarks

idl-code-gen: # regenerate protobuf stubs from protos/dex.proto into server + SDKs
	$(MAKE) -C ../protos proto

idl-code-gen-server: # server rewrite: regenerate only server/gen/dexpb (leave SDK trees alone)
	$(MAKE) -C ../protos proto-server-go

bins: $(BINS)

serverCommandIntegTests:
	@cd ../web && npm ci && npm run build
	@cd cmd/server && go test -tags=integration ./... -count=1 -v

clean: ## Clean binaries and build folder
	rm -f $(BINS)
	rm -Rf $(BUILD)
	$(if \
		$(filter $(BIN)/fake-codegen, $(wildcard $(BIN)/*)), \
		$(warning fake build tools may exist, delete the $(BIN) folder to get real ones if desired),)

deps: ## Check for dependency updates, for things that are directly imported
	@make --no-print-directory DEPS_FILTER='$(JQ_DEPS_ONLY_DIRECT)' deps-all

deps-all: ## Check for all dependency updates
	@go list -u -m -json all \
		| $(JQ_DEPS_AGE) \
		| sort -n

cleanTestCache:
	$Q go clean -testcache

integrationCoverageDir ?=
integrationCoverageFlags = $(if $(integrationCoverageDir),-covermode=atomic -coverpkg=./service/...)
integrationCoverageArgs = $(if $(integrationCoverageDir),-args -test.gocoverdir="$(abspath $(integrationCoverageDir))")

prepareIntegrationCoverage:
	$(if $(integrationCoverageDir),$Q mkdir -p "$(abspath $(integrationCoverageDir))")

integrationCoverage:
	$Q set -e; \
		coverage_raw_dir="$$(mktemp -d)"; \
		trap 'rm -rf "$$coverage_raw_dir"' EXIT; \
		$(MAKE) --no-print-directory integTests integrationCoverageDir="$$coverage_raw_dir"; \
		./script/merge-integ-coverage.sh coverage "$$coverage_raw_dir"

integTests: prepareIntegrationCoverage
	$Q go test $(integrationCoverageFlags) -v ./integ -timeout 20m $(integrationCoverageArgs)

temporalIntegTests: prepareIntegrationCoverage
	$Q go test $(integrationCoverageFlags) -v ./integ -timeout 20m -cadence=false $(integrationCoverageArgs)

streamIntegTests: prepareIntegrationCoverage
	$Q go test $(integrationCoverageFlags) -v ./integ -timeout 5m -run '^TestStream' -cadence=false -dependencyWaitSeconds=30 $(integrationCoverageArgs)

streamBenchmarks:
	$Q go test ./integ -run '^$$' -bench '^BenchmarkStreamStoreWrite$$' -benchmem -temporal=false -cadence=false -dependencyWaitSeconds=0

dexServerAddress ?= 127.0.0.1:8801
temporalHostPort ?= 127.0.0.1:7233
skipContinueAsNewTests ?= true

temporalIntegTestsAgainstLocalDexDev: prepareIntegrationCoverage
	$Q set -e; \
		test_run_pattern='.'; \
		if [ "$(skipContinueAsNewTests)" = "true" ]; then \
			test_run_pattern="$$(go test -v ./integ -list '^Test' -temporal=false -cadence=false -dependencyWaitSeconds=0 | awk '/^Test/ && $$0 !~ /ContinueAsNew/ { tests = tests separator $$0; separator = "|" } END { print "^(" tests ")$$" }')"; \
		fi; \
		go test $(integrationCoverageFlags) -v ./integ -timeout 20m -run "$$test_run_pattern" -search=false -cadence=false -dexServerAddress=$(dexServerAddress) -temporalHostPort=$(temporalHostPort) $(integrationCoverageArgs)

cadenceIntegTests: prepareIntegrationCoverage
	$Q go test $(integrationCoverageFlags) -v ./integ -timeout 20m -temporal=false $(integrationCoverageArgs)

ci-web-api-temporal-test: prepareIntegrationCoverage
	$Q go test $(integrationCoverageFlags) -v ./integ -timeout 5m -run '^TestWebAPITemporal$$' -search=false -cadence=false -dependencyWaitSeconds=60 -temporalHostPort=$(temporalHostPort) $(integrationCoverageArgs)

ci-web-api-cadence-test: prepareIntegrationCoverage
	$Q go test $(integrationCoverageFlags) -v ./integ -timeout 5m -run '^TestWebAPICadence$$' -search=false -temporal=false -dependencyWaitSeconds=180 $(integrationCoverageArgs)

totalPartitions ?= 1
partitionNum ?= 0

ci-cadence-integ-test: prepareIntegrationCoverage
	$Q set -e; \
		test_run_pattern="$$(./script/select-integ-test-partition.sh "$(totalPartitions)" "$(partitionNum)" integ/cadence-ci-tests.txt)"; \
		go test $(integrationCoverageFlags) -v ./integ -timeout 20m -run "$$test_run_pattern" -search=false -temporal=false -dependencyWaitSeconds=180 $(integrationCoverageArgs)

ci-cadence-integ-test-disable-sticky: prepareIntegrationCoverage
	$Q go test $(integrationCoverageFlags) -v ./integ -run "(?i)^Test[${startsWith}]" -search=false -temporal=false -dependencyWaitSeconds=180 -disableStickyCache $(integrationCoverageArgs)

ci-temporal-integ-test: prepareIntegrationCoverage
	$Q set -e; \
		test_run_pattern="$$(./script/select-integ-test-partition.sh "$(totalPartitions)" "$(partitionNum)")"; \
		go test $(integrationCoverageFlags) -v ./integ -timeout 20m -run "$$test_run_pattern" -search=false -cadence=false -dependencyWaitSeconds=60 $(integrationCoverageArgs)

ci-temporal-integ-test-disable-sticky: prepareIntegrationCoverage
	$Q go test $(integrationCoverageFlags) -v ./integ -timeout 20m -run "(?i)^Test[${startsWith}]" -search=false -cadence=false -dependencyWaitSeconds=60 -disableStickyCache $(integrationCoverageArgs)

ci-all-tests:
	$Q go test -v ./... -timeout 20m -cover -coverprofile coverage.out -coverpkg ./service/...

integTestsNoSearch: prepareIntegrationCoverage
	$Q go test $(integrationCoverageFlags) -v ./integ -search=false -timeout 20m $(integrationCoverageArgs)

unitTests:
	@GOWORK=off go test -v ./config $$(GOWORK=off go list ./service/... | grep -v ./service/common/blobstore)
	@GOWORK=off go test -v ./service/common/blobstore -run '^TestDeterministicBlobObjectID'

# Requires MySQL, PostgreSQL, Temporal, and Cadence in label-gated CI.
attributeSyncIntegTests: prepareIntegrationCoverage
	@GOWORK=off go test $(integrationCoverageFlags) -v -tags=attributestore_integration ./service/common/attributestore -run '^TestMySQLAndPostgresAttributeStore.*Integration$$' -timeout 5m $(integrationCoverageArgs)
	@GOWORK=off go test $(integrationCoverageFlags) -v -tags=attributestore_integration ./integ -run '^TestAttributeSync' -timeout 10m -temporalHostPort=$(temporalHostPort) $(integrationCoverageArgs)

# Requires MongoDB and covers both workflow backends in label-gated CI.
mongodbAttributeStoreIntegTests: prepareIntegrationCoverage
	@GOWORK=off go test $(integrationCoverageFlags) -v -tags=attributestore_integration ./service/common/attributestore -run '^TestMongoDBAttributeStoreIntegration$$' -timeout 5m $(integrationCoverageArgs)
	@GOWORK=off go test $(integrationCoverageFlags) -v -tags=attributestore_integration ./integ -run '^TestMongoDBAttributeSync' -timeout 10m -temporalHostPort=$(temporalHostPort) $(integrationCoverageArgs)

blobStoreIntegTests: prepareIntegrationCoverage
	@GOWORK=off go test $(integrationCoverageFlags) -v ./service/common/blobstore -run '^(TestBlobStoreIntegration|TestLocalBlobStore|TestS3AttributeBlobCache)' -timeout 5m $(integrationCoverageArgs)

localBlobStoreTests: prepareIntegrationCoverage
	@GOWORK=off go test $(integrationCoverageFlags) -v ./service/common/blobstore -run '^TestLocalBlobStore' $(integrationCoverageArgs)

replayTests:
	@GOWORK=off go test -v ./replayTests -timeout 10m

help:
	@# print help first, so it's visible
	@printf "\033[36m%-20s\033[0m %s\n" 'help' 'Prints a help message showing any specially-commented targets'
	@# then everything matching "target: ## magic comments"
	@cat $(MAKEFILE_LIST) | grep -e "^[a-zA-Z_\-]*:.* ## .*" | awk 'BEGIN {FS = ":.*? ## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' | sort
