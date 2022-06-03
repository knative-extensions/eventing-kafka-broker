# Knative Eventing Kafka Channel

## Motivation for the channel

This repository contains another KafkaChannel implementation, which we believe is more flexible and scalable.

There are already 2 other Kafka Channel implentations:
the [consolidated KafkaChannel](https://github.com/knative-sandbox/eventing-kafka/blob/main/pkg/channel/consolidated/README.md)
and
the [distributed KafkaChannel](https://github.com/knative-sandbox/eventing-kafka/blob/main/pkg/channel/distributed/README.md)
.

While working on these 2 channel implementations, we realized we could reuse the dataplane for other Knative Kafka
components, namely Kafka Broker and the Kafka Sink.

The dataplane is designed in a way that it only communicates the control plane via a configmap and thus it is:
* More loosely-coupled
* Easier to test standalone
* Using a different stack that we find more suitable for the dataplane

## Comparison with other KafkaChannel implementations

You may find a comparison of this new KafkaChannel implementation with
the [consolidated KafkaChannel](https://github.com/knative-sandbox/eventing-kafka/blob/main/pkg/channel/consolidated/README.md)
and
the [distributed KafkaChannel](https://github.com/knative-sandbox/eventing-kafka/blob/main/pkg/channel/distributed/README.md)
implementations.

|                         | New KafkaChannel                                        | Consolidated KafkaChannel                               | Distributed KafkaChannel                                                             |
|-------------------------|---------------------------------------------------------|---------------------------------------------------------|--------------------------------------------------------------------------------------|
| Delivery Guarantees     | At least once                                           | At least once                                           | At least once                                                                        |
| Scalability             | Ingress and egress are in different pods and scalable** | Ingress and egress are in the same pod and not scalable | Ingress and egress are in different pods but not scalable                            |
| Data plane multitenancy | All KafkaChannel resources leverage the same dataplane  | All KafkaChannel resources leverage the same dataplane  | All KafkaChannel resources leverage the same ingress but each has a dedicated egress |
| Offset repositioning    | Not supported                                           | Not supported                                           | Supported                                                                            |
| Dataplane stack         | Java (Vert.x)                                           | Go (Sarama)                                             | Go (Sarama)                                                                          |

** Work in progress

## Migration

Automated migration is possible for migrations from the consolidated channel implementation to the new implementation.
Distributed channel users need to migrate manually.

### Breaking changes from consolidated channel

#### Channel config

New implementation uses a new configmap called `kafka-channel-config`. It has a different structure as well.

The consolidated channel config looks like this:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: config-kafka
  namespace: knative-eventing
data:
  version: 1.0.0
  sarama: |
    enableLogging: false
    config: |
      Version: 2.0.0 # Kafka Version Compatibility From Sarama's Supported List (Major.Minor.Patch)
      Admin:
        Timeout: 10000000000  # 10 seconds
      Net:
        KeepAlive: 30000000000  # 30 seconds
        MaxOpenRequests: 1 # Set to 1 for use with Idempotent Producer
        TLS:
          Enable: true
        SASL:
          Enable: true
          Version: 1
      Metadata:
        RefreshFrequency: 300000000000  # 5 minutes
      Consumer:
        Offsets:
          AutoCommit:
            Interval: 5000000000  # 5 seconds
          Retention: 604800000000000  # 1 week
      Producer:
        Idempotent: true  # Must be false for Azure EventHubs
        RequiredAcks: -1  # -1 = WaitForAll, Most stringent option for "at-least-once" delivery.
  eventing-kafka: |
    cloudevents:
      maxIdleConns: 1000
      maxIdleConnsPerHost: 100
    kafka:
      authSecretName: kafka-cluster
      authSecretNamespace: knative-eventing
      brokers: my-cluster-kafka-bootstrap.kafka:9092
    channel:
      adminType: kafka # One of "kafka", "azure", "custom"
      dispatcher:
        cpuRequest: 100m
        memoryRequest: 50Mi
      receiver:
        cpuRequest: 100m
        memoryRequest: 50Mi
```

A sample config for the new channel is available below:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-channel-config
  namespace: knative-eventing
data:
  bootstrap.servers: "my-cluster-kafka-bootstrap.kafka:9092"
  auth.secret.ref.name: kafka-cluster
  auth.secret.ref.namespace: knative-eventing
```

Most of the options are not available in the new channel implementation because we are not using Sarama in the new dataplane
anymore. Other options are not available because they are set to sensible defaults at this moment.

#### Secret

The new channel implementation requires a new key in the secret called `protocol`.

In the consolidated channel implementation, `protocol` was inferred from the available information in the secret. However,
in the new channel, we ask users to explicitly define the protocol.

Using SASL without SSL look like this in the previous secret format:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: strimzi-tls-secret
  namespace: knative-eventing
type: Opaque
data:
  ca.crt: ...
  user.crt: ...
  user.key: ...
```

New implementation requires specifying the protocol explicitly:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: strimzi-tls-secret
  namespace: knative-eventing
type: Opaque
data:
  ca.crt: ...
  user.crt: ...
  user.key: ...
  protocol: ...   # NEW
```

Possible values for the protocol are:

- `PLAINTEXT`: No SASL and no SSL is used
- `SSL`: Only SSL is used
- `SASL_PLAINTEXT`: Only SASL is used
- `SASL_SSL`: Both SASL and SSL are used

New channel implementation can use the existing secrets as is. However, the old secret format will be deprecated soon,
and it is advised to modify your secret.

#### Services

Consolidated channel controller creates a service per channel resource. This channel service is used as the address
of the channel itself, and it will forward the events to the dispatcher. As each channel resource has its own service,
they will also have a separate host name and the dispatcher will be able to identify the channel by looking at its
hostname.

In the new KafkaChannel implementation, this is preserved. However, in the future versions, we will switch from host-based-routing
to path-based-routing. We will use a single hostname for the channel ingress and identify channels by the URL paths.

#### Controller leases

The new channel implementation's controllers use the same lease name as the previous controllers. This provides
a nice and smooth transition to new controllers with a roll-out.

### Auto migration from consolidated channel

In 1.15 version of the new channel implementation, there is an automated migration post-install job available for
the migration from the consolidated channel.

This job:

- Deletes unnecessary resources (old webhook, old hpa, old serviceAccount, old roles)
- Copies over relevant parts of old configmap (`config-kafka`) to new configmap (`kafka-channel-config`)
- Adjusts the channel services (the services that are created per channel instance) to use new channel ingress
- Eventually, deletes the old configmap (`config-kafka`) and the old deployments

All configuration options in the previous configmap are ignored, except these:

* `eventing-kafka.kafka.brokers`: Copied over to `bootstrap.servers` in  new configmap
* `eventing-kafka.kafka.authSecretName`: Copied over to `auth.secret.ref.name` in  new configmap
* `eventing-kafka.kafka.authSecretNamespace`: Copied over to `auth.secret.ref.namespace` in  new configmap

Secret is not modified and the `protocol` information is inferred from what's available in the secret and the
previous configmap. However, this inferring will be deprecated and removed soon.

## Configuring dataplane

Configmap `config-kafka-channel-data-plane` contains 4 different keys to configure dataplane defaults.

- `config-kafka-channel-producer.properties`: Passed to underlying [Vert.x Kafka Client](https://vertx.io/docs/vertx-kafka-client/java/) when creating a Kafka producer
- `config-kafka-channel-consumer.properties`: Passed to underlying [Vert.x Kafka Client](https://vertx.io/docs/vertx-kafka-client/java/) when creating a Kafka consumer
- `config-kafka-channel-webclient.properties`: Passed to underlying [Vert.x Web Client](https://vertx.io/docs/vertx-web-client/java/) when creating a web client that does the http requests to subscribers
- `config-kafka-channel-httpserver.properties`: Passed to [Vert.x core](https://vertx.io/docs/vertx-core/java/#_writing_http_servers_and_clients) when creating an HTTP server for the ingress

Values of these keys are in [.properties format](https://en.wikipedia.org/wiki/.properties#Format).

You may find the default value of this configmap [in the repository](https://github.com/knative-sandbox/eventing-kafka-broker/blob/main/data-plane/config/channel/100-config-kafka-channel-data-plane.yaml).


## Best practices

If you are using [Knative `MTChannelBasedBroker` Broker](https://knative.dev/docs/eventing/configuration/broker-configuration/#channel-implementation-options)
backed by a KafkaChannel, you might consider using [KafkaBroker](https://knative.dev/docs/eventing/broker/kafka-broker/) instead.
This will save you additional HTTP hops, from channel to broker and broker to channel.

## Discontinued features

- [Namespace dispatchers](https://github.com/knative-sandbox/eventing-kafka/blob/main/pkg/channel/consolidated/README.md#namespace-dispatchers)
  are not supported at the moment.
