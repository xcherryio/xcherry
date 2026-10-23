# Monorepo root helpers. Component builds stay in server/, sdk-*/, samples-*/.

ROOT_DIR := $(abspath .)
GENERATED_CODE_PATHS := \
	server/gen/dexpb \
	sdk-go/gen/dexpb \
	sdk-java/src/main/java/io/superdurable/gen \
	sdk-python/dex/dexpb \
	sdk-typescript/src/gen

.PHONY: help ci-runner-check copyright copyright-check generated-code generated-code-check githooks docs-prose-check docs-prose-fix release-tooling-test

release-tooling-test: ## Verify changed-component selection and release preflight
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s script/release -p '*_test.py'

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-22s %s\n", $$1, $$2}'

githooks: ## Install commit-msg and pre-commit hooks (Cursor co-author + docs prose)
	bash script/install-githooks

docs-prose-check: ## Fail when product-doc MDX prose uses inline backticks
	python3 script/docs_no_inline_backticks.py

docs-prose-fix: ## Replace inline backticks with **bold** in product-doc MDX prose
	python3 script/docs_no_inline_backticks.py --fix

ci-runner-check: ## Verify CI workflows use GitHub-hosted runners
	bash .github/scripts/check-ci-runners.sh

copyright: ## Add or upgrade license headers using the legacy manifest
	cd server && GOWORK=off go run ./cmd/tools/copyright -rootDir "$(ROOT_DIR)"

copyright-check: ## Verify license classifications, body hashes, and headers
	cd server && GOWORK=off go run ./cmd/tools/copyright -rootDir "$(ROOT_DIR)" -verifyOnly

generated-code: ## Regenerate all checked-in code derived from protos
	$(MAKE) -C protos proto

generated-code-check: generated-code ## Verify checked-in generated code is current
	@changes="$$(git status --short --untracked-files=all -- $(GENERATED_CODE_PATHS))"; \
	if [ -n "$$changes" ]; then \
		echo "Generated code is outdated. Run 'make generated-code' and commit every change:"; \
		printf '%s\n' "$$changes"; \
		exit 1; \
	fi

.PHONY: rust-rules-check
rust-rules-check: ## Check repository-wide Rust conventions and checker fixtures
	python3 script/check-rust-rules.py
	python3 -m unittest discover -s script -p check_rust_rules_test.py
