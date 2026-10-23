.PHONY: check test clippy fmt-check run

# Example: make run ARGS="--data-directory /tmp/s3mock-data"

check:
	cargo check --all-targets --locked

test:
	cargo test --all-targets --locked

clippy:
	cargo clippy --all-targets --locked -- -D warnings

fmt-check:
	cargo fmt --all --check

run:
	cargo run --locked --release --bin s3mock -- $(ARGS)
