.PHONY : build
build :
	cargo build

.PHONY : setup
setup :
	rustup component add rustfmt
	rustup component add clippy

.PHONY : format
format :
	cargo fmt --

.PHONY : lint
lint :
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features -- \
			-D warnings \
			-A clippy::upper-case-acronyms

.PHONY : test
test :
	@cargo test --workspace --lib
	@cargo test --workspace --doc
	@cargo test --test codegen task_codegen
	@cargo test --no-run --test codegen app_codegen
	@cargo test --no-run --test codegen beat_codegen

.PHONY : broker-amqp-test
broker-amqp-test :
	@cargo test --test integrations brokers::amqp

.PHONY : rabbitmq
rabbitmq :
	@./scripts/brokers/amqp.sh

.PHONY : doc
doc :
	cargo doc --workspace
