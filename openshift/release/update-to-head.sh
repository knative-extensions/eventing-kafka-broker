#!/usr/bin/env bash

# Synchs the REPO_BRANCH branch to main and then triggers CI
# Usage: update-to-head.sh

set -e
REPO_NAME="eventing-kafka-broker"
REPO_OWNER_NAME="openshift-knative"
REPO_BRANCH="release-next"
REPO_BRANCH_CI="${REPO_BRANCH}-ci"

# Check if there's an upstream release we need to mirror downstream
openshift/release/mirror-upstream-branches.sh

# Reset REPO_BRANCH to upstream/main.
git fetch upstream main
git checkout upstream/main -B ${REPO_BRANCH}

# Update openshift's main and take all needed files from there.
git fetch openshift main
git checkout openshift/main openshift OWNERS Makefile

# Remove GH Action hooks from upstream
rm -rf .github/workflows
git commit -sm ":fire: remove unneeded workflows" .github/

# Generate our OCP artifacts
make generate-dockerfiles
make RELEASE=ci generate-release
git add openshift OWNERS Makefile
git commit -m ":open_file_folder: Update openshift specific files."

# Apply patches if present
PATCHES_DIR="$(pwd)/openshift/patches/"
if [ -d "$PATCHES_DIR" ] && [ "$(ls -A "$PATCHES_DIR")" ]; then
    git apply openshift/patches/*
    make RELEASE=ci generate-release
    git commit -am ":fire: Apply carried patches."
fi
git push -f openshift ${REPO_BRANCH}

# Trigger CI
git checkout ${REPO_BRANCH} -B ${REPO_BRANCH_CI}
date > ci
git add ci
git commit -m ":robot: Triggering CI on branch '${REPO_BRANCH}' after synching to upstream/main"
git push -f openshift ${REPO_BRANCH_CI}

if hash hub 2>/dev/null; then
   # Test if there is already a sync PR in 
   COUNT=$(hub api -H "Accept: application/vnd.github.v3+json" repos/${REPO_OWNER_NAME}/${REPO_NAME}/pulls --flat \
    | grep -c ":robot: Triggering CI on branch '${REPO_BRANCH}' after synching to upstream/main") || true
   if [ "$COUNT" = "0" ]; then
      hub pull-request --no-edit -l "kind/sync-fork-to-upstream" -b ${REPO_OWNER_NAME}/${REPO_NAME}:${REPO_BRANCH} -h ${REPO_OWNER_NAME}/${REPO_NAME}:${REPO_BRANCH_CI}
   fi
else
   echo "hub (https://github.com/github/hub) is not installed, so you'll need to create a PR manually."
fi
