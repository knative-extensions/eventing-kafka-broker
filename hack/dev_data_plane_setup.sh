#!/usr/bin/env bash

# create a docker-based development setup

# generate protobuf code
./proto/hack/generate_proto || exit 1

# start a docker container with maven installed.
# usage:
# 1. cd /app/data-plane
# 2. mvn <command-to-execute>
docker run -ti --rm \
  -v ~/.m2:/var/maven/.m2 \
  -v "$(pwd)":/app \
  --user "$(id -g)":"$(id -u)" \
  -e MAVEN_CONFIG=/var/maven/.m2 maven:3.6.3-openjdk-14 /bin/bash
