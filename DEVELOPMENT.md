# Development

This document describes the usual development loop for hacking on this project.

## Prerequisites

- Read [Contributing page](./CONTRIBUTING.md)

## Set up a Kubernetes cluster

- [Using KinD (Kubernetes in Docker)](#create-a-kind-cluster-optional)
- Using a different Kubernetes cluster: https://kubernetes.io/docs/setup/

### Create a KinD cluster (Optional)

_Note: If you're not using `KinD`, you can skip this step._

```shell
make kind-up
```

## Installation

This guide assumes you have a Kubernetes cluster up and running and reachable via `kubectl`.

### Before you begin

### Container registry

We use a container registry to publish container images, so we need to point the environment variable `KO_DOCKER_REPO`
to your own registry.

For example, to use Docker Hub, we can run the following command:

```shell
export KO_DOCKER_REPO=docker.io/<username>
```

_Note: replace `<username>` with your username._

If you're using `KinD`, you can use its local registry by setting the variable `KO_DOCKER_REPO` to `kind.local`:

```shell
export KO_DOCKER_REPO=kind.local
```

### Install dependencies (Eventing and Kafka) and publish test images

```shell
make deps-up
```

### Install Eventing Kafka Broker

```shell
make up
```

### Run unit tests

```shell
make ut
```

_Note: These tests do not require a running Kubernetes cluster._

### Run unit tests

```shell
make ut
```

If you want to run only data plane unit tests run the following command:

```shell
make dp-ut
```

Or, alternatively, if you want to run only control plane unit tests run the following command:

```shell
make cp-ut
```

_Note: These tests do not require a running Kubernetes cluster._

### Run build tests

```shell
make bt
```

_Note: These tests do not require a running Kubernetes cluster._

### Run integration and E2E tests

```shell
make it
```

### Hack, build, test and iterate

Once we have everything up and running we can start writing code, building, testing and iterating again and again.

We can run the following command to apply newly added changes to our Kubernetes cluster:

```shell
make up
```

### Run Sacura test

Sacura is a tool designed to continuously send and receive CloudEvents at a configurable pace.

We use it to test that our components don't lose events for a given period of time.

_Note: This test requires at least 4Gi of memory_

```shell
make sacura-test
```

Once the test completes, run the following command to clean up sacura resources:

```shell
make sacura-down
```

### Run Chaos test

Our integration tests run while a continuous pod killer, called `chaosduck`, is running to detect high availability
issues.

_Note: `chaosduck` is heavyweight since it requires that every component has multiple replicas._

```shell
make chaos-up
```

`chaosduck` itself does nothing other than killing leader pods, so at this point you should run our E2E tests.

Once you want to stop `chaosduck`, run the following command:

```shell
make chaos-down
```

### Run profiler

We run `async-profiler` in CI to allow a reviewer to check that newly added changes of a PR are reasonable in terms of
resource usages.

The profiler can measure some events like `alloc`, `cpu`, etc, and we need to specify which event we want to measure.

_Note: this command requires root privileges, and it can only run on Linux._

```shell
export EVENT=alloc
make profiler
```

For more information on the profiler test, see [the profiler test doc](./data-plane/profiler/README.md).

## Code generation

Sometimes, before running `make up` to rebuild and apply newly added changes, it's required to run our code generators,
by running the following command:

```shell
make generate
```

This step is required only if we're changing a protobuf definition file, or for some specific API changes.

If you're unsure, whether you need to run it or not for your changes, run [our build tests](#run-build-tests).

### Protobuf compiler

One of our code generator is the protobuf compiler, so if we're changing a protobuf definition file, we need to run the
following command:

```shell
make generate-proto
```

## Teardown

### Delete dependencies

```shell
make deps-down
```

### Delete Eventing Kafka Broker

```shell
make down
```

### Delete KinD cluster

```shell
make kind-down
```
