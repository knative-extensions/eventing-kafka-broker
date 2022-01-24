#!/bin/bash

# Usage: create-release-branch.sh v0.4.1 release-0.4

set -ex # Exit immediately on error.

release=$1
target=$2

# Fetch the latest tags and checkout a new branch from the wanted tag.
git fetch upstream -v --tags
git checkout -b "$target" "$release"

# Copy the openshift extra files from the OPENSHIFT/main branch.
git fetch openshift main
git checkout openshift/main -- openshift OWNERS Makefile

# Remove GH Action hooks from upstream
rm -rf .github/workflows
git commit -sm ":fire: remove unneeded workflows" .github/

# Generate our OCP artifacts
make generate-dockerfiles
make RELEASE=$release generate-release
git add openshift OWNERS Makefile
git commit -m "Add openshift specific files."

# Apply patches if present
PATCHES_DIR="$(pwd)/openshift/patches/"
if [ -d "$PATCHES_DIR" ] && [ "$(ls -A "$PATCHES_DIR")" ]; then
    git apply openshift/patches/*
    make RELEASE=$release generate-release
    git commit -am ":fire: Apply carried patches."
fi
