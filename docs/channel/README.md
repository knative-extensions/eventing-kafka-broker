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

TBD
TBD
TBD
TBD
TBD
TBD
TBD
TBD
TBD
TBD
TBD
TBD
TBD

### Auto migration from consolidated channel

All configuration options in the previous configmap are ignored, except these:

* `eventing-kafka.kafka.brokers`: Copied over to `bootstrap.servers` in  new configmap
* `eventing-kafka.kafka.authSecretName`: Copied over to `auth.secret.ref.name` in  new configmap
* `eventing-kafka.kafka.authSecretNamespace`: Copied over to `auth.secret.ref.namespace` in  new configmap

Secret is not modified and the `protocol` information is inferred from what's available in the secret and the
previous configmap. However, this inferring will be deprecated and removed soon.
