# Consumers Scheduling

## Goals

- Improve Scalability
  - Remove limit on the number of triggers per broker - https://github.com/knative-sandbox/eventing-kafka-broker/issues/337
  - Consumers partitioning and Trigger parallelism https://github.com/knative-sandbox/eventing-kafka-broker/issues/785
- Give to the event consumers control on the number of replicas (delivery parallelism)
- Minimal changes to the data plane
- Ease the implementation of a new MT `KafkaSource`

## Introduction

The `Trigger` and `KafkaSource` controller will schedule resources by using the `Placeable` duck type.

```go
// Placeable is a list of podName and virtual replicas pairs.
// Each pair represents the assignment of virtual replicas to a pod
type Placeable struct {
	MaxAllowedVReplicas *int32      `json:"maxAllowedVReplicas,omitempty"`
	Placement           []Placement `json:"placements,omitempty"`
}

type Placement struct {
	// PodName is the name of the pod where the resource is placed
	PodName string `json:"podName,omitempty"`
	// VReplicas is the number of virtual replicas assigned to in the pod
	VReplicas int32 `json:"vreplicas,omitempty"`
}
```

The `KafkaSource` type embeds the `Placeable` type in the status, while the Trigger will use
a different representation of that type in the `status.annotations` field like the following:

```yaml
apiVersion: eventing.knative.dev/v1
kind: Trigger
# ...
metadata:
  annotations:
    # This is set by the Trigger author to signal how many consumers they want to run.
    kafka.eventing.knative.dev/replicas: <num_replicas>
status:
  annotations:
    # Embed placeable type in status annotations: 
    # - https://github.com/knative-sandbox/eventing-kafka/blob/main/pkg/apis/duck/v1alpha1/placement_types.go
    kafka.eventing.knative.dev/scheduler.placeable.MaxAllowedVReplicas: num_topic_partitions
    kafka.eventing.knative.dev/scheduler.placeable.Placements.0.PodName: kafka-broker-dispatcher-abc
    kafka.eventing.knative.dev/scheduler.placeable.Placements.0.VReplicas: <num_replicas_for_pod_kafka-broker-dispatcher-abc>
    kafka.eventing.knative.dev/scheduler.placeable.Placements.1.PodName: kafka-broker-dispatcher-xyz
    kafka.eventing.knative.dev/scheduler.placeable.Placements.1.VReplicas: <num_replicas_for_pod_kafka-broker-dispatcher-xyz>
    # ...
```

Once a resource reconciler made a scheduling decision a new reconciler will actually schedule resources to dispatcher pods.
We call this new reconciler the [`Pod` reconciler](#Pod-Reconciler) since it will reconcile `corev1.Pod`s.

## Resource Reconcilers

Resource reconcilers are the `Trigger`, `KafkaSource`, and `KafkaChannel` reconciler.

High level flow of the resource reconcilers will be:

1. Schedule the consumer replicas into available pods by updating the `Placeable` status
1. Reconcile the resource `Spec` (addressable sink, dead letter sink, etc)
1. Mark the resource ready when the `Pod` reconciler has seen the updated scheduling decision

## Pod Reconciler

The pod reconciler will reconcile `dispatcher` pods.

A pod will be reconciled when a `placeable`, that references a pod, changes.

The high level flow of the `Pod` recoconciler is:

1. Get all resources (Trigger or KafkaSource) assigned to it
1. Transform them to the internal contract representation
1. Save the contract in the ConfigMap associtated with the pod
1. Update pod annotations as follow:
    ```yaml
    apiVersion: v1
    kind: Pod
    metadata:
    annotations:
        # <resource_namespace>/<resource_name> represents the resource key
        kafka.eventing.knative.dev/scheduler.placeable.Placements.<resource_namespace>/<resource_name>: <resource.status.ObservedGeneration>
    # ...
    # A single ConfigMap per dispatcher pod is mounted here by the webhook
    ```
1. The annotation update will trigger a refresh of the mounted `ConfigMap`

## Coordination

A resource reconciler (Trigger, KafkaSource, etc) needs to know when the `Pod` Reconcile has reconciled a given resource to mark it as `Ready`.

To mark a resource, identified by `<resource_namespace>/<resource_name>`, as ready these conditions must all be true:

1. To make sure that the `Pod` reconciler has seen the latest resource scheduling decision, we need that, for each existing pod where the resource was scheduled check that:
   `kafka.eventing.knative.dev/scheduler.placeable.Placements.<resource_namespace>/<resource_name> >= resource.status.ObservedGeneration`
1. To make sure that there are no further changes to the scheduled resource, we need to check that: `metadata.generation == resource.status.ObservedGeneration`

In other words, the `Pod` reconciler has seen the `Trigger` reconciler scheduling decision.

## Known Issues and TODOs:

- Current scheduler implementation in `eventing-kafka` depends on `StatefulSet`
  - We can:
    - Remove the dependency on `StatefulSet`
    - Move the dispatcher to use a `StatefulSet` (lower effort)
    - Create our own scheduler
- KafkaChannel:
    - How to embed `N` placeables one per subscription in `KafkaChannel` status
    - How to get `Subscription` replicas

## PoC status

- branch: https://github.com/pierDipi/eventing-kafka-broker/tree/poc-scheduling
  - Important files to look at:
    - trigger_v2.go
    - trigger_v2_pod_controller.go
    - trigger_v2_vpod.go
    - controller_v2.go
