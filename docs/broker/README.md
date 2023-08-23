# Knative Eventing Broker for Apache Kafka

This document gives a rough overview over the architecture of the Knative Eventing Broker for Apache Kafka.

![Overview](eventing-kafka-broker.png)

## Control Plane

The `kafka-controller` is kind of the heart of the Broker for Apache Kafkas control plane and has the following responsibilities:

* Watch specific resources and incorporate relevant details about the resource into the `kafka-broker-brokers-triggers` configmap (step 3 in the diagram), including:
  * `Broker`s with the `eventing.kantive.dev/broker.class: Kafka` annotation (step 2 in the diagram),
  * `Trigger`s referencing a Knative Eventing Broker for Apache Kafka,
  * `KafkaChannel`s,
  * `KafkaSink`s 
  * `KafkaSource`s
* Update the status on `Broker` resources that have the `eventing.kantive.dev/broker.class: Kafka` annotation with the ingress address of the broker.

The `kafka-broker-brokers-triggers` configmap contains information about the resources in the [contract.proto](https://github.com/knative-extensions/eventing-kafka-broker/blob/main/proto/contract.proto) schema. This makes it easily accessible to other components, such as those operating in the data plane.

## Data Plane

The data plane of the Knative Eventing Broker for Apache Kafka exists of two main components: `kafka-broker-receiver` and `kafka-broker-dispatcher`.

### kafka-broker-receiver

The `kafka-broker-receiver` takes requests for new CloudEvents and persists them in an Apache Kafka Topic. It gets the topic name for the corresponding Knative Eventing Broker from the volume mounting the `kafka-broker-brokers-triggers` configmap.

### kafka-broker-dispatcher

The `kafka-broker-dispatcher` polls periodically Apache Kafka for new messages, wraps them into CloudEvents, eventually filters them and sends them to the egresses of the broker resource (an egress can be a Subscriber of the broker for example). It gets the list of egresses from the volume mounting the `kafka-broker-brokers-triggers` configmap. The `kafka-broker-dispatcher` also handles replies and retries. 

### Core Classes

* [FileWatcher](https://github.com/knative-extensions/eventing-kafka-broker/blob/main/data-plane/core/src/main/java/dev/knative/eventing/kafka/broker/core/file/FileWatcher.java): The `FileWatcher`, listens for changes on the contract file (volume mounting the `kafka-broker-brokers-triggers` configmap) and sends events about updates to the (vertx) event bus.
* [ResourcesReconcilerMessageHandler](https://github.com/knative-extensions/eventing-kafka-broker/blob/main/data-plane/core/src/main/java/dev/knative/eventing/kafka/broker/core/reconciler/impl/ResourcesReconcilerMessageHandler.java): The `ResourcesReconcilerMessageHandler` listens for events from the `FileWatcher` on the event bus and triggers the `ResourcesReconciler` to reconcile the resource. It gets created from the receiver (`ReceiverVerticle`) and dispatcher (`ConsumerDeployerVerticle`) through some factories.
* [ResourcesReconciler](https://github.com/knative-extensions/eventing-kafka-broker/blob/main/data-plane/core/src/main/java/dev/knative/eventing/kafka/broker/core/reconciler/ResourcesReconciler.java): The `ResourcesReconciler` reconciles a resource based on the contract file.
* [IngressReconcilerListener](https://github.com/knative-extensions/eventing-kafka-broker/blob/main/data-plane/core/src/main/java/dev/knative/eventing/kafka/broker/core/reconciler/IngressReconcilerListener.java): The `IngressReconcilerListener` is setup by the receiver (`ReceiverVerticle`) to reconcile the ingresses of the resources.
* [EgressReconcilerListener](https://github.com/knative-extensions/eventing-kafka-broker/blob/main/data-plane/core/src/main/java/dev/knative/eventing/kafka/broker/core/reconciler/EgressReconcilerListener.java): The `EgressReconcilerListener` is setup by the dispatcher (`ConsumerDeployerVerticle`) to reconcile the egresses of the resources.
