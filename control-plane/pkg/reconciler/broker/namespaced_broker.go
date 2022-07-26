/*
 * Copyright 2022 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package broker

import (
	"context"
	"fmt"

	mf "github.com/manifestival/manifestival"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/utils/pointer"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

// TODO: 1.
// TODO: Read config-kafka-broker-data-plane in user namespace first, fallback to the one in knative-eventing.
// TODO: Though, this would mean that we need to mount the configmap from knative-eventing to the pod in user namespace.
// TODO: This is not supported by Kubernetes. If we want this, we need to read the configmap content dynamically, with
// TODO: necessary RBAC.

// TODO: 2.
// TODO: config-kafka-broker-data-plane is created by the reconciler dynamically.
// TODO: This means, the contents can ONLY be changed by users after a broker is created.
// TODO: Also, reconciler will reconcile it when it is changed.

// TODO: 3.
// TODO: config-tracing and config-logging has the same mounting issues. We need to read these manually.

// TODO: 4.
// TODO: Do we reconcile everything when one of the configmaps change?
// TODO: Specifically, contract configmap, etc.

// TODO: 5.
// TODO: IPListerWithMapping struct can leak resources in case of a leader change
// TODO: We might need to create a TTL mechanism

type NamespacedReconciler struct {
	*base.Reconciler
	*config.Env

	Resolver *resolver.URIResolver

	ConfigMapLister corelisters.ConfigMapLister

	// NewKafkaClusterAdminClient creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc

	BootstrapServers string

	// BaseDataPlaneManifest is a Manifestival manifest that has the resources that need to be created in the user namespace.
	BaseDataPlaneManifest mf.Manifest

	Prober prober.Prober

	IPsLister prober.IPListerWithMapping
}

func (r *NamespacedReconciler) ReconcileKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	r.IPsLister.Register(
		types.NamespacedName{Namespace: broker.Namespace, Name: broker.Name},
		prober.GetIPForService(types.NamespacedName{Namespace: broker.Namespace, Name: r.Env.IngressName}),
	)

	br := r.createReconcilerForBrokerInstance(broker)

	manifest, err := r.getManifest(broker)
	if err != nil {
		return fmt.Errorf("unable to transform dataplane manifest. namespace: %s, broker: %v, error: %v", broker.Namespace, broker, err)
	}

	if err = manifest.Apply(); err != nil {
		return fmt.Errorf("unable to apply dataplane manifest. namespace: %s, broker: %v, error: %v", broker.Namespace, broker, err)
	}

	return br.ReconcileKind(ctx, broker)
}

func (r *NamespacedReconciler) FinalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	br := r.createReconcilerForBrokerInstance(broker)
	result := br.FinalizeKind(ctx, broker)

	r.IPsLister.Unregister(types.NamespacedName{Namespace: broker.Namespace, Name: broker.Name})

	return result
}

func (r *NamespacedReconciler) createReconcilerForBrokerInstance(broker *eventing.Broker) *Reconciler {

	return &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:             r.Reconciler.KubeClient,
			PodLister:              r.Reconciler.PodLister,
			SecretLister:           r.Reconciler.SecretLister,
			SecretTracker:          r.Reconciler.SecretTracker,
			ConfigMapTracker:       r.Reconciler.ConfigMapTracker,
			DataPlaneConfigMapName: r.Reconciler.DataPlaneConfigMapName,
			DataPlaneConfigFormat:  r.Reconciler.DataPlaneConfigFormat,
			DispatcherLabel:        r.Reconciler.DispatcherLabel,
			ReceiverLabel:          r.Reconciler.ReceiverLabel,

			DataPlaneNamespace:          broker.Namespace,
			DataPlaneConfigMapNamespace: broker.Namespace,

			DataPlaneConfigMapTransformer: func(cm *corev1.ConfigMap) {
				kafka.NamespacedDataplaneLabelConfigmapOption(cm)
				appendBrokerAsOwnerRef(broker)(cm)
			},
		},
		Env:                        r.Env,
		Resolver:                   r.Resolver,
		ConfigMapLister:            r.ConfigMapLister,
		NewKafkaClusterAdminClient: r.NewKafkaClusterAdminClient,
		BootstrapServers:           r.BootstrapServers,
		Prober:                     r.Prober,
	}
}

// getManifest returns the manifest that is transformed from the BaseDataPlaneManifest with the changes
// for the given broker
func (r *NamespacedReconciler) getManifest(broker *eventing.Broker) (mf.Manifest, error) {
	manifest := r.BaseDataPlaneManifest

	manifest, err := manifest.Transform(mf.InjectNamespace(broker.Namespace))
	if err != nil {
		return mf.Manifest{}, fmt.Errorf("unable to transform base dataplane manifest with namespace injection. namespace: %s, error: %v", broker.Namespace, err)
	}

	return manifest.Transform(appendNewOwnerRefsToPersisted(manifest.Client, broker))
}

func appendNewOwnerRefsToPersisted(client mf.Client, broker *eventing.Broker) mf.Transformer {
	return func(resource *unstructured.Unstructured) error {
		existingRefs, err := getPersistedOwnerRefs(client, resource)
		if err != nil {
			return err
		}

		refs, _ := appendOwnerRef(existingRefs, broker)
		// We need to set this even when we don't append any ownerRef.
		// That is because the resource is in a pristine state that's read from the YAML, and we need to set the
		// existing owners to that in memory resource.
		resource.SetOwnerReferences(refs)
		return nil
	}
}

func getPersistedOwnerRefs(mfc mf.Client, res *unstructured.Unstructured) ([]metav1.OwnerReference, error) {
	retrieved, err := mfc.Get(res)
	if err != nil && !errors.IsNotFound(err) {
		// actual problem
		return nil, fmt.Errorf("failed to get resource %s/%s: %w", res.GetNamespace(), res.GetName(), err)
	}

	if errors.IsNotFound(err) {
		// not found, return empty slice
		return []metav1.OwnerReference{}, nil
	}

	// found, return existing ownerRefs
	return retrieved.GetOwnerReferences(), nil
}

func appendBrokerAsOwnerRef(broker *eventing.Broker) base.ConfigMapOption {
	return func(cm *corev1.ConfigMap) {
		existingRefs := cm.GetOwnerReferences()
		if refs, appended := appendOwnerRef(existingRefs, broker); appended {
			cm.SetOwnerReferences(refs)
		}
	}
}

// appendOwnerRef appends the ownerRef for the given broker to the ownerRefs slice, if it doesn't exist already
func appendOwnerRef(refs []metav1.OwnerReference, broker *eventing.Broker) ([]metav1.OwnerReference, bool) {
	for i := range refs {
		if refs[i].UID == broker.UID {
			return refs, false
		}
	}

	newRef := *metav1.NewControllerRef(broker, broker.GetGroupVersionKind())
	newRef.Controller = pointer.Bool(false)
	newRef.BlockOwnerDeletion = pointer.Bool(true)

	return append(refs, newRef), true
}

func setImagesForDeployments(imageMap map[string]string) mf.Transformer {
	return func(resource *unstructured.Unstructured) error {
		if resource.GetKind() != "Deployment" {
			return nil
		}

		var deployment = &appsv1.Deployment{}
		if err := scheme.Scheme.Convert(resource, deployment, nil); err != nil {
			return err
		}

		for i := range deployment.Spec.Template.Spec.Containers {
			if img, exists := imageMap[deployment.Spec.Template.Spec.Containers[i].Image]; exists {
				deployment.Spec.Template.Spec.Containers[i].Image = img
				continue
			}
		}

		return scheme.Scheme.Convert(deployment, resource, nil)
	}
}
