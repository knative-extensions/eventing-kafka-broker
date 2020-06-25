# Architecture

The document [Alternative Broker Implementation based on Apache Kafka](https://docs.google.com/document/d/10-qylWrj7Tj81EqoIiZ2AANAZNmkKe7XKmFobgZULKY/edit)
explains the architecture, and some reasons for implementing a native Kafka Broker instead of using a channel-based Broker. 
(You need to join `knative-dev` group to view it).

The **data-plane** is implemented in Java for leveraging the always up-to-date, feature rich and tuned Kafka client.
The **control-plane** is implemented in Go following the 
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

- `data-plane` directory contains data-plane components (`receiver` and `dispatcher`).
- `control-plane` directory contains control-plane reconcilers (`Broker` and `Trigger`). 
- `hack` directory contains scripts for updating dependencies, generated code, etc.
- `proto` directory contains `protobuf` definitions of messages for 
    **control-plane** `<->` **data-plane** communication.
- `test` directory contains end to end tests and associated scripts for running them. 
- `thirdy_party` contains dependencies licences
- `vendor` directory contains vendored Go dependencies.