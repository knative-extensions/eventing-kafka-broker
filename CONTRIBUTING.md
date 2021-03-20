# Contribution guidelines

So you want to hack on Knative Eventing Kafka Broker? Yay! Please refer to Knative's overall
[contribution guidelines](https://www.knative.dev/contributing/) to find out how you can help.

## Getting started

1. [Create and checkout a repo fork](#checkout-your-fork)

### Requirements

You need to install:

- [`ko`](https://github.com/google/ko) - (_required_)
- [`docker`](https://www.docker.com/) - (_required_)
- [`Go`](https://golang.org/) - (_required_)
    - check
      [go \<version\>](https://github.com/knative-sandbox/eventing-kafka-broker/blob/master/go.mod)
      for the required Go version used in this project
- [`Java`](https://www.java.com/en/) (we recommend an `openjdk` build) -
  (_required_)
    - check
      [java.version](https://github.com/knative-sandbox/eventing-kafka-broker/blob/master/data-plane/pom.xml)
      maven property for the required Java version used in this project

Requirements signaled as "optional" are not required, but it's highly recommended having them installed. If a specific
version of a requirement is not explicitly defined above, any version will work during development.

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

Once you reach this point you are ready to do a full build and deploy as follows.

- [Development](DEVELOPMENT.md)
