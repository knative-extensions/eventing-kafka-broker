# Data-plane

The data-plane uses [Vertx](https://vertx.io/) and is composed of two components:

- [**Receiver**](#receiver), it's responsible for accepting incoming events and send them to the appropriate Kafka
  topic. It acts as Kafka producers and broker ingress.
- [**Dispatcher**](#dispatcher), it's responsible for consuming events and send them to Triggers' subscribers. It acts
  as Kafka consumer.

## Receiver

The receiver starts an HTTP server, and it accepts requests with a path of the form
`/<broker-namespace>/<broker-name>/`.

Once a request comes, it sends the event in the body to the topic `knative-broker-<broker-namespace>-<broker-name>`.

## Dispatcher

The dispatcher starts a file watcher, which watches changes to a mounted ConfigMap. Such ConfigMap contains
configurations of Brokers and Triggers in the cluster. (see [proto/contract.proto](../proto/contract.proto))

For each Trigger it creates a Kafka consumer with `group.id=<trigger_id>`, which is then wrapped in a Vert.x verticle.

When it detects a Trigger update or deletion the consumer associated with that Trigger will be closed, and in case of an
update another one will be created. This allows to not block or use locks.

### Directory structure

```bash
.
├── config
├── core
├── dispatcher
├── generated
├── receiver
```

- `config` directory contains Kubernetes artifacts (yaml).
- `core` directory contains the core module, in particular, it contains classes for representing Eventing objects
- `dispatcher` directory contains the [_Dispatcher_](#dispatcher) application.
- `contract` directory contains a module in which the protobuf compiler (`protoc`) generates code. Git ignores the
  generated code.
- `receiver` directory contains the [_Receiver_](#receiver) application.
