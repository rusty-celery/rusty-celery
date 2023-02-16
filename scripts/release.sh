#!/bin/bash

set -e

toml_set () {
    out=$(toml set "$1" "$2" "$3")
    echo "$out" > "$1"
}

toml_get () {
    toml get --raw "$1" "$2"
}

current_version=$(toml_get Cargo.toml package.version)

read -p "Current version is $current_version, enter new version: " version
tag="v${version}"

read -p "Creating new release for $tag. Do you want to continue? [Y/n] " prompt

if [[ $prompt == "y" || $prompt == "Y" || $prompt == "yes" || $prompt == "Yes" ]]; then
    echo "Updating Cargo.toml files and CHANGELOG..."
else
    echo "Cancelled"
    exit 1
fi

toml_set Cargo.toml package.version "$version"
toml_set Cargo.toml dependencies.celery-codegen.version "$version"
toml_set celery-codegen/Cargo.toml package.version "$version"

# Make sure sub-crate versions match.
if [[ "$version" != $(toml_get celery-codegen/Cargo.toml package.version) ]]; then
    echo 'Version in celery-codegen/Cargo.toml does not match!'
    exit 1
fi
if [[ "$version" != $(toml_get Cargo.toml dependencies.celery-codegen.version) ]]; then
    echo 'Version for celery-codegen dependency does not match!'
    exit 1
fi

VERSION="$version" python scripts/prepare_changelog.py

read -p "Updated files, please check for errors. Do you want to continue? [Y/n] " prompt

if [[ $prompt == "y" || $prompt == "Y" || $prompt == "yes" || $prompt == "Yes" ]]; then
    git add -A
    git commit -m "(chore) Bump version to $tag for release" || true && git push
    git tag "$tag" -m "$tag"
    git push --tags
else
    echo "Cancelled"
    exit 1
fi
