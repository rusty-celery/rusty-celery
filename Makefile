.PHONY : setup
setup :
	rustup component add rustfmt
	rustup component add clippy

#
# Cargo helpers.
#

.PHONY : build
build :
	cargo build

.PHONY : release
release :
	cargo build --release

.PHONY : format
format :
	cargo fmt --

.PHONY : lint
lint :
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings

.PHONY : test
test :
	cargo test

.PHONY : doc
doc :
	cargo doc

.PHONY : all-checks
all-checks : lint test doc

.PHONY : publish
publish :
	cargo publish

.PHONY : readme
readme :
	cargo readme > README.md

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
