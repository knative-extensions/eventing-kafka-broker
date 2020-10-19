#!/usr/bin/env bash

# Usage:
#
# Generate a GitHub token: https://github.com/settings/tokens (no permissions required)
# export GITHUB_TOKEN=<your_github_token>
#
# Run the following command to install release-notes (https://github.com/kubernetes/release/tree/master/cmd/release-notes)
# GO111MODULE=on go get k8s.io/release/cmd/release-notes (to avoid updating the go.mod file, run it in a directory outside the project)
#
# Run the following commands to generate the `release-notes.md` file.
#
# export START_SHA=<start_git_commit_sha>
# export END_SHA=<end_git_commit_sha>
# ./hack/release-notes.sh

release-notes \
  --org knative-sandbox \
  --repo eventing-kafka-broker \
  --required-author="" \
  --repo-path "$(pwd)" \
  --output release-notes.md
