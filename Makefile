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
			-A clippy::let_and_return \
			-A clippy::redundant_clone

.PHONY : test
test :
	@cargo test --workspace --lib
	@cargo test --workspace --doc
	@cargo test --test codegen

.PHONY : broker-amqp-test
broker-amqp-test :
	@cargo test --test integrations broker::amqp

.PHONY : rabbitmq
rabbitmq :
	@docker run -p 5672:5672 --rm -e RABBITMQ_DEFAULT_VHOST=my_vhost rabbitmq:3

.PHONY : doc
doc :
	cargo doc --workspace

level = alpha

.PHONY : release
release :
	cargo release --workspace $(level)

#
# Git helpers.
#

.PHONY: create-branch
create-branch :
ifneq ($(issue),)
	git checkout -b ISSUE-$(issue)
	git push --set-upstream origin $$(git branch | grep \* | cut -d ' ' -f2)
else ifneq ($(name),)
	git checkout -b $(name)
	git push --set-upstream origin $$(git branch | grep \* | cut -d ' ' -f2)
else
	$(error must supply 'issue' or 'name' parameter)
endif

.PHONY : delete-branch
delete-branch :
	@BRANCH=`git rev-parse --abbrev-ref HEAD` \
		&& [ $$BRANCH != 'master' ] \
		&& echo "On branch $$BRANCH" \
		&& echo "Checking out master" \
		&& git checkout master \
		&& git pull \
		&& echo "Deleting branch $$BRANCH" \
		&& git branch -d $$BRANCH \
		&& git remote prune origin
