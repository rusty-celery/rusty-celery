#!/bin/bash

set -e

VERSION=$(toml get --raw 'Cargo.toml' 'package.version')
TAG="v${VERSION}"

# Make sure sub-crate versions match.
if [[ "$VERSION" != $(toml get --raw 'celery-codegen/Cargo.toml' 'package.version') ]]; then
    echo 'Version in celery-codegen/Cargo.toml does not match!'
    exit 1
fi
if [[ "$VERSION" != $(toml get --raw 'Cargo.toml' 'dependencies.celery-codegen.version') ]]; then
    echo 'Version for celery-codegen dependency does not match!'
    exit 1
fi

read -p "Creating new release for $TAG. Do you want to continue? [Y/n] " prompt

if [[ $prompt == "y" || $prompt == "Y" || $prompt == "yes" || $prompt == "Yes" ]]; then
    VERSION="$VERSION" python scripts/prepare_changelog.py
    git add -A
    git commit -m "(chore) Bump version to $TAG for release" || true && git push
    echo "Creating new git tag $TAG"
    git tag "$TAG" -m "$TAG"
    git push --tags
else
    echo "Cancelled"
    exit 1
fi
