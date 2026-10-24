.PHONY: check test clippy fmt-check integTests flowSmokeTests checkServerZdExclusions

# server-zd rejects these samples, so DEX_INTEG_ENGINE=zd filters them out by exact test name.
SERVER_ZD_INTEG_FAILURE_POLICY_TESTS := \
	money_transfer_completes_with_released_sdk \
	order_processing_happy_path \
	order_processing_reminder_then_ship \
	order_processing_ship_failure_refunds \
	failure_recovery_retries_and_compensates \
	polling_streams_job_progress_and_completes_with_job_result
SERVER_ZD_FLOW_SMOKE_FAILURE_POLICY_TESTS := \
	flow_smoke_products_money_transfer \
	flow_smoke_products_order_processing \
	flow_smoke_patterns_polling \
	flow_smoke_patterns_manual_recovery \
	flow_smoke_patterns_recovery \
	flow_smoke_primitives_step_options_override
SERVER_ZD_FLOW_SMOKE_ATTRIBUTE_STORE_SYNC_TESTS := \
	flow_smoke_patterns_entity_store

ifeq ($(DEX_INTEG_ENGINE),zd)
INTEG_ENGINE_FILTER_ARGS := --exact \
	$(addprefix --skip ,$(SERVER_ZD_INTEG_FAILURE_POLICY_TESTS))
FLOW_SMOKE_ENGINE_FILTER_ARGS := --exact \
	$(addprefix --skip ,$(SERVER_ZD_FLOW_SMOKE_FAILURE_POLICY_TESTS)) \
	$(addprefix --skip ,$(SERVER_ZD_FLOW_SMOKE_ATTRIBUTE_STORE_SYNC_TESTS))
endif

# $(1) lists test names and $(2) names the feature server-zd does not support.
define print_server_zd_exclusions
	@if [ "$(DEX_INTEG_ENGINE)" = zd ]; then \
		for test_name in $(1); do \
			echo "Excluded test $$test_name: server-zd does not support $(2)"; \
		done; \
	fi
endef

check:
	cargo check --all-targets --locked

test: checkServerZdExclusions
	cargo test --all-targets --locked

integTests:
	$(call print_server_zd_exclusions,$(SERVER_ZD_INTEG_FAILURE_POLICY_TESTS),failure policies)
	cargo test --locked --test dex_integration -- --ignored --test-threads=1 $(INTEG_ENGINE_FILTER_ARGS) $(TEST_RUNNER_ARGS)

flowSmokeTests:
	$(call print_server_zd_exclusions,$(SERVER_ZD_FLOW_SMOKE_FAILURE_POLICY_TESTS),failure policies)
	$(call print_server_zd_exclusions,$(SERVER_ZD_FLOW_SMOKE_ATTRIBUTE_STORE_SYNC_TESTS),Attribute Store sync)
	cargo test --locked --test flow_smoke -- --ignored --test-threads=1 $(FLOW_SMOKE_ENGINE_FILTER_ARGS) $(TEST_RUNNER_ARGS)

checkServerZdExclusions:
	./check-server-zd-exclusions.sh

clippy:
	cargo clippy --all-targets --locked -- -D warnings

fmt-check:
	cargo fmt --all --check
