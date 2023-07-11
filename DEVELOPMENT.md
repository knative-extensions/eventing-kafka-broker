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
kind create cluster
```

Alternatively you can use [./hack/create-kind-cluster.sh](hack/create-kind-cluster.sh) to setup a kind cluster with a
local registry. This local registry will then be available by default at `localhost:5001`.

```shell
./hack/create-kind-cluster.sh
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

In case you setup `KinD` via [./hack/create-kind-cluster.sh](hack/create-kind-cluster.sh), the local registry is
available at `localhost:5001`:

```shell
export KO_DOCKER_REPO=localhost:5001
```

### Install dependencies (Eventing and Kafka) and publish test images

```shell
./hack/run.sh deploy-infra
```

### Install Eventing Kafka Broker

```shell
./hack/run.sh deploy
```

### Run build tests

```shell
./hack/run.sh build-tests
```

_Note: These tests do not require a running Kubernetes cluster._

### Run unit tests

```shell
./hack/run.sh unit-tests
```

If you want to run only data plane unit tests run the following command:

```shell
./hack/run.sh unit-tests-data-plane
```

Or, alternatively, if you want to run only control plane unit tests run the following command:

```shell
./hack/run.sh unit-tests-control-plane
```

_Note: These tests do not require a running Kubernetes cluster._

### Run integration and E2E tests

_Note: This test requires at least 5Gi of memory_

```shell
LOCAL_DEVELOPMENT=true SKIP_INITIALIZE=true ./test/e2e-tests.sh
```

### Hack, build, test and iterate

Once we have everything up and running we can start writing code, building, testing and iterating again and again.

We can run the following command to apply newly added changes to our Kubernetes cluster:

```shell
./hack/run.sh deploy
```

### Run Sacura test

Sacura is a tool designed to continuously send and receive CloudEvents at a configurable pace.

We use it to test that our components don't lose events for a given period of time.

_Note: This test requires at least 4Gi of memory_

```shell
./hack/run.sh deploy-sacura
./hack/run.sh sacura-test
```

Once the test completes, run the following command to clean up sacura resources:

```shell
./hack/run.sh teardown-sacura
```

### Run Chaos test

Our integration tests run while a continuous pod killer, called `chaosduck`, is running to detect high availability
issues.

_Note: `chaosduck` is heavyweight since it requires that every component has multiple replicas._

```shell
./hack/run.sh deploy-chaos
```

`chaosduck` itself does nothing other than killing leader pods, so at this point you should run our E2E tests.

Once you want to stop `chaosduck`, run the following command:

```shell
./hack/run.sh teardown-chaos
```

### Run profiler

We run `async-profiler` in CI to allow a reviewer to check that newly added changes of a PR are reasonable in terms of
resource usages.

The profiler can measure some events like `alloc`, `cpu`, etc, and we need to specify which event we want to measure.

_Note: this command requires root privileges, and it can only run on Linux._

```shell
export EVENT=alloc
./hack/run.sh profiler
```

For more information on the profiler test, see [the profiler test doc](./data-plane/profiler/README.md).

## Code generation

Sometimes, before deploying the services it's required to run our code generators, by running the following command:

```shell
./hack/run.sh generate
```

This step is required only if we're changing a protobuf definition file, or for some specific API changes.

If you're unsure, whether you need to run it or not for your changes, run [our build tests](#run-build-tests).

## Code Formatting

### Go

We are using [go fmt](https://pkg.go.dev/fmt) to format our Go code. To format your code, run the following
command within the `control-plane` directory:

```shell
go fmt ./...
```

### Java

We are using Spotless to format our Java code. To format your code, run the following command within the `data-plane`
directory:

```shell
mvn spotless:apply
```

You can also configure it to your IDE. For more information, please refer
to [Spotless](https://github.com/diffplug/spotless).

This action is also performed **automatically** when you run `./hack/run.sh generate`.

## Teardown

### Delete dependencies

```shell
./hack/run.sh teardown-infra
```

### Delete Eventing Kafka Broker

```shell
./hack/run.sh teardown
```

### Delete KinD cluster

```shell
kind delete cluster
```

### Building From Source

```shell
./hack/run build-from-source
```

The project root directory will contain artifacts that are part of the released assets.
