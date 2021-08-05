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

.PHONY : check-fmt
check-fmt :
	cargo fmt --all -- --check

.PHONY : check-clippy
check-clippy :
	cargo clippy --workspace --all-targets --all-features -- \
			-D warnings \
			-A clippy::upper-case-acronyms

.PHONY : lint
lint : check-fmt check-clippy

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

.PHONY : run-all-tests
run-all-tests :
	@cargo test --workspace --lib
	@cargo test --workspace --doc
	@cargo test --test codegen task_codegen
	@cargo test --no-run --test codegen app_codegen
	@cargo test --no-run --test codegen beat_codegen
	@cargo test --test integrations brokers::amqp
	@cargo test --test integrations brokers::redis

.PHONY : rabbitmq
rabbitmq :
	@./scripts/brokers/amqp.sh

.PHONY : build-docs
build-docs :
	cargo doc --all-faetures --workspace --no-deps
