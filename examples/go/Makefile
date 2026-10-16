MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

# Preserve the example module's published SDK and Connector versions.
export GOWORK := off

.PHONY: bins clean dealDSLDemo e2eTests fmt tests triggerDealDSLDemo unitTests

default: help

bins:
	go build -o dex-samples ./cmd/server
	go build -o dex-deal-dsl ./cmd/deal-dsl

clean:
	rm -f dex-samples dex-deal-dsl

dealDSLDemo:
	./run-deal-dsl-demo.sh

triggerDealDSLDemo:
	./trigger-deal-dsl-demo.sh

e2eTests:
	./run-e2e-tests.sh

fmt:
	gofmt -s -w ./cmd ./integ ./products ./patterns ./production ./registry ./server ./shared

tests: unitTests

unitTests:
	go test -v ./cmd/... ./products/... ./patterns/... ./registry/...

help:
	@printf "\033[36m%-20s\033[0m %s\n" 'help' 'Prints this help message'
	@grep -E "^[a-zA-Z_-]+:.*" $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":"}; {printf "\033[36m%-20s\033[0m\n", $$1}' | sort -u
