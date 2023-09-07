# Dispatcher & Receiver Loom Implementations

Loom implemantation of Dispatcher and Receiver are located in [`dispatcher-loom`](https://github.com/knative-extensions/eventing-kafka-broker/tree/main/data-plane/dispatcher-loom) and [`receiver-loom`](https://github.com/knative-extensions/eventing-kafka-broker/tree/main/data-plane/receiver-loom) directories respectively.  
In these implementations we use [Project Loom](https://openjdk.org/projects/loom/)'s virtual threads for the underlyig Kafka communication asynchronously.

> **Note:** This is still in early access and is not yet production ready. It might replace the current implementation in the future. but that will not effect the API or the functionality of the broker.

## Installation

**Pre-requisites:** You have followed the [CONTRIBUTING](https://github.com/knative-extensions/eventing-kafka-broker/blob/main/CONTRIBUTING.md) & [DEVELOPMENT](https://github.com/knative-extensions/eventing-kafka-broker/blob/main/DEVELOPMENT.md) guides and have a Kubernetes cluster up and running and reachable.

To install you can run the following command:
    
```shell
./hack/run.sh deploy-loom
```

OR  

Export the environment variable `USE_LOOM=true`

```shell
export USE_LOOM=true
```
And run any command that is available in the [run.sh](https://github.com/knative-extensions/eventing-kafka-broker/blob/main/hack/run.sh) script or any other command that you want to run.