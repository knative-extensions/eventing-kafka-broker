# Architecture

The document
[Alternative Broker Implementation based on Apache Kafka](https://docs.google.com/document/d/10-qylWrj7Tj81EqoIiZ2AANAZNmkKe7XKmFobgZULKY/edit)
explains the architecture, and some reasons for implementing a native Kafka
Broker instead of using a channel-based Broker. (You need to join `knative-dev`
group to view it).

The **data-plane** is implemented in Java for leveraging the always up-to-date,
feature rich and tuned Kafka client. The **control-plane** is implemented in Go
following the
[Knative Kubernetes controllers](https://github.com/knative-sandbox/sample-controller).

- data-plane internals: [data-plane/README.md](data-plane/README.md).
<!--- TODO add control-plane internals --->

# Directory structure

```bash
.
├── data-plane
├── control-plane
├── hack
├── proto
├── test
└── vendor
```

- `data-plane` directory contains data-plane components (`receiver` and
  `dispatcher`).
- `control-plane` directory contains control-plane reconcilers (`Broker` and
  `Trigger`).
- `hack` directory contains scripts for updating dependencies, generated code,
  etc.
- `proto` directory contains `protobuf` definitions of messages for
  **control-plane** `<->` **data-plane** communication.
- `test` directory contains end to end tests and associated scripts for running
  them.
- `thirdy_party` contains dependencies licences
- `vendor` directory contains vendored Go dependencies.

# Collecting Prometheus metrics

<!-- TODO Move this to knative/docs and add control plane configs (separate sink and broker stuff) -->

<!-- prettier-ignore-start -->
```yaml
    # scrape kafka-broker-receiver
    - job_name: kafka-broker-receiver
      scrape_interval: 5m
      scrape_timeout: 30s
      kubernetes_sd_configs:
      - role: pod
      relabel_configs:
      # Scrape only the the targets matching the following metadata
      - source_labels: [__meta_kubernetes_namespace, __meta_kubernetes_pod_label_app, __meta_kubernetes_pod_container_port_name]
        action: keep
        regex: knative-eventing;kafka-broker-receiver;http-metrics
      # Rename metadata labels to be reader friendly
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_service_name]
        target_label: service
    # scrape kafka-broker-dispatcher
    - job_name: kafka-broker-dispatcher
      scrape_interval: 5m
      scrape_timeout: 30s
      kubernetes_sd_configs:
      - role: pod
      relabel_configs:
      # Scrape only the the targets matching the following metadata
      - source_labels: [__meta_kubernetes_namespace, __meta_kubernetes_pod_label_app, __meta_kubernetes_pod_container_port_name]
        action: keep
        regex: knative-eventing;kafka-broker-dispatcher;http-metrics
      # Rename metadata labels to be reader friendly
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_service_name]
        target_label: service
    # scrape kafka-sink-receiver
    - job_name: kafka-sink-receiver
      scrape_interval: 5m
      scrape_timeout: 30s
      kubernetes_sd_configs:
      - role: pod
      relabel_configs:
      # Scrape only the the targets matching the following metadata
      - source_labels: [__meta_kubernetes_namespace, __meta_kubernetes_pod_label_app, __meta_kubernetes_pod_container_port_name]
        action: keep
        regex: knative-eventing;kafka-sink-receiver;http-metrics
      # Rename metadata labels to be reader friendly
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_service_name]
        target_label: service
```
<!-- prettier-ignore-end -->
