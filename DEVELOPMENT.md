# Development

This doc explains how to set up a development environment, so you can get started contributing.
Also, take a look at:

- [The pull request workflow](https://www.knative.dev/contributing/contributing/#pull-requests)

## Getting started

1. [Create and checkout a repo fork](#checkout-your-fork)

Before submitting a PR, see also [contribution guidelines](./CONTRIBUTING.md).

### Requirements

You need to install:

- [`ko`](https://github.com/google/ko) - (_required_)
- [`docker`](https://www.docker.com/) - (_required_)
- [`Go`](https://golang.org/) - (_required_)
- [`Java`](https://www.java.com/en/) (we recommend an `openjdk` build) - (_optional_)
- [`Maven`](https://maven.apache.org/) - (_optional_)

Requirements signaled as "optional" are not required, but it's highly recommended having them installed.

### Create a cluster and a repo

1. [Set up a kubernetes cluster](https://www.knative.dev/docs/install/)
   - Follow an install guide up through "Creating a Kubernetes Cluster"
   - You do _not_ need to install Istio or Knative using the instructions in the
     guide. Simply create the cluster and come back here.
   - If you _did_ install Istio/Knative following those instructions, that's
     fine too, you'll just redeploy over them, below.
1. Set up a Linux Container registry for pushing images. You can use any
   container image registry by adjusting the authentication methods and
   repository paths mentioned in the sections below.

> :information_source: You'll need to be authenticated with your
> `KO_DOCKER_REPO` before pushing images.

### Setup your environment

To start your environment you'll need to set these environment variables (we
recommend adding them to your `.bashrc`):

1. `KO_DOCKER_REPO`: The docker repository to which developer images should be pushed.

`.bashrc` example:

```shell
export GOPATH="$HOME/go"
export PATH="${PATH}:${GOPATH}/bin"
export KO_DOCKER_REPO=docker.io/<your_docker_id>
# export KO_DOCKER_REPO=gcr.io/<your_gcr_id>
```

### Checkout your fork

To check out this repository:

1. Create your own [fork of this repository](https://help.github.com/articles/fork-a-repo/):
1. Clone it to your machine:

```shell
mkdir -p ${GOPATH}/src/knative.dev
cd ${GOPATH}/src/knative.dev
git clone git@github.com:knative/eventing.git # clone eventing repo
git clone git@github.com:${YOUR_GITHUB_USERNAME}/eventing-kafka-broker.git
cd eventing-kafka-broker
git remote add upstream https://github.com/knative-sandbox/eventing-kafka-broker.git
git remote set-url --push upstream no_push
```

_Adding the `upstream` remote sets you up nicely for regularly
[syncing your fork](https://help.github.com/articles/syncing-a-fork/)._

Once you reach this point you are ready to do a full build and deploy as
follows.

# Code generation

Run `./proto/hack/generate_proto` to generate protobuf code.

# Deploy core configurations and Kafka

```bash
# re-execute the script in case some errors appear. 
# (this can happen when the CRDs hasn't been registered and we try to create a Kafka cluster)
./test/kafka/kafka_setup.sh 

kubectl apply -f config
```

# Changing the data-plane

- The [./hack/dev_data_plane_setup.sh](hack/dev_data_plane_setup.sh) script sets up a container with maven.
    The script contains instructions on how to use it, and what it does.
    
If you are using [KinD](https://kind.sigs.k8s.io/) we recommend executing:

```bash
export WITH_KIND=true
export SKIP_PUSH=true
```

This loads images in KinD and skips the push to the remote registry pointed by `KO_DOCKER_REPO`, allowing speeding up
the development cycle.
    
- Execute `source test/data-plane/library.sh`.
- Execute `data_plane_build_push` to build and push data-plane images (`SKIP_PUSH=true` will skip the push).
- Execute `k8s apply --force`

# Changing the control-plane

<!--- TODO add instruction for iterating on the control-plane --->

# E2E Tests

Running E2E tests as you make changes to the code-base is pretty simple. 
See [the test docs](./test/README.md).

# Contributing

Please check [contribution guidelines](./CONTRIBUTING.md).

## Clean up

<!--- TODO add instruction for clean up control-plane --->

```bash
k8s delete # assume `source test/data-plane/library.sh`
kubectl delete -f --ignore-not-found config/
```
