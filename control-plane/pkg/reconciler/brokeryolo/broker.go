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

package brokeryolo

import (
	"context"
	"fmt"
	mf "github.com/manifestival/manifestival"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/pointer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	brokerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
)

// TODO: 1.
// TODO: Read config-kafka-broker-data-plane in user namespace first, fallback to the one in knative-eventing.
// TODO: Though, this would mean that we need to mount the configmap from knative-eventing to the pod in user namespace.
// TODO: This is not supported by Kubernetes. If we want this, we need to read the configmap content dynamically, with
// TODO: necessary RBAC.

// TODO: 2.
// TODO: config-kafka-broker-data-plane is created by the reconciler dynamically.
// TODO: This means, the contents can be changed after a broker is created.
// TODO: Also, reconciler will reconcile it when it is changed.

// TODO: 3.
// TODO: config-tracing and config-logging has the same mounting issues. We need to read these manually.

// TODO: 4.
// TODO: Do we reconcile everything when one of the configmaps change?
// TODO: Specifically, contract configmap, etc.

type Reconciler struct {
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

func (r *Reconciler) ReconcileKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	logging.FromContext(ctx).Infof("YOLO ReconcileKind (wrapper)")
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, broker)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	r.IPsLister.Register(
		types.NamespacedName{Namespace: broker.Namespace, Name: broker.Name},
		prober.GetIPForService(types.NamespacedName{Namespace: broker.Namespace, Name: r.Env.IngressName}),
	)

	br := r.createReconcilerForBrokerInstance(ctx, broker)

	err := r.ReconcileDataPlaneConfigMap(ctx, broker)
	if err != nil {
		return fmt.Errorf("failed to create/update data plane config map: %s, error: %v", fmt.Sprintf("%s/%s", broker.Namespace, r.Env.DataPlaneConfigMapName), err)
	}

	manifest, err := r.getManifestForNamespace(broker.Namespace)
	if err != nil {
		return err
	}

	manifest, err = manifest.Transform(appendNewOwnerRefsToPersisted(manifest.Client, broker))
	if err != nil {
		return err
	}

	if r.Env.DispatcherImage == "" {
		return fmt.Errorf("unable to find DispatcherImage env var specified with 'BROKER_DISPATCHER_IMAGE'")
	}
	if r.Env.ReceiverImage == "" {
		return fmt.Errorf("unable to find DispatcherImage env var specified with 'BROKER_RECEIVER_IMAGE'")
	}

	manifest, err = manifest.Transform(setImagesForDeployments(map[string]string{
		"${KNATIVE_KAFKA_DISPATCHER_IMAGE}": r.Env.DispatcherImage,
		"${KNATIVE_KAFKA_RECEIVER_IMAGE}":   r.Env.ReceiverImage,
	}))
	if err != nil {
		return err
	}

	err = manifest.Apply()
	if err != nil {
		return fmt.Errorf("unable to apply dataplane manifest. namespace: %s, owner: %v, error: %v", broker.Namespace, broker, err)
	}

	return br.DoReconcileKind(ctx, broker)
}

func (r *Reconciler) FinalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	logging.FromContext(ctx).Infof("YOLO FinalizeKind (wrapper)")
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, broker)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	logging.FromContext(ctx).Infof("YOLO finalizeKind")

	br := r.createReconcilerForBrokerInstance(ctx, broker)
	result := br.DoFinalizeKind(ctx, broker)

	r.IPsLister.Unregister(types.NamespacedName{Namespace: broker.Namespace, Name: broker.Name})

	return result
}

func (r *Reconciler) createReconcilerForBrokerInstance(ctx context.Context, broker *eventing.Broker) *brokerreconciler.Reconciler {

	return &brokerreconciler.Reconciler{
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

			SystemNamespace:             broker.Namespace,
			DataPlaneConfigMapNamespace: broker.Namespace,
		},
		Env:                        r.Env,
		Resolver:                   r.Resolver,
		ConfigMapLister:            r.ConfigMapLister,
		NewKafkaClusterAdminClient: r.NewKafkaClusterAdminClient,
		BootstrapServers:           r.BootstrapServers,
		Prober:                     r.Prober,
	}
}

func (r *Reconciler) getManifestForNamespace(namespace string) (mf.Manifest, error) {
	manifest := r.BaseDataPlaneManifest

	manifest, err := manifest.Transform(mf.InjectNamespace(namespace))
	if err != nil {
		return mf.Manifest{}, fmt.Errorf("unable to transform base dataplane manifest with namespace injection. namespace: %s, error: %v", namespace, err)
	}

	return manifest, nil
}

func appendNewOwnerRefsToPersisted(client mf.Client, broker *eventing.Broker) mf.Transformer {
	return func(resource *unstructured.Unstructured) error {
		existingRefs, err := getPersistedOwnerRefs(client, resource)
		if err != nil {
			return err
		}

		if refs, appended := appendOwnerRef(existingRefs, broker); appended {
			resource.SetOwnerReferences(refs)
		}

		return nil
	}
}

func getPersistedOwnerRefs(mfc mf.Client, res *unstructured.Unstructured) ([]metav1.OwnerReference, error) {
	retrieved, err := mfc.Get(res)
	if err != nil && !errors.IsNotFound(err) {
		// actual problem
		return nil, err
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

	refs = append(refs, newRef)

	return refs, true
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

// TODO: copy paste!
func (r *Reconciler) ReconcileDataPlaneConfigMap(ctx context.Context, broker *eventing.Broker) error {
	cm, err := r.KubeClient.CoreV1().
		ConfigMaps(broker.Namespace).
		Get(ctx, r.Env.DataPlaneConfigMapName, metav1.GetOptions{})

	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if apierrors.IsNotFound(err) {
		// not found, create
		_, err = r.createDataPlaneConfigMap(ctx, broker.Namespace, appendBrokerAsOwnerRef(broker))
		return err
	}

	// found, update

	if refs, appended := appendOwnerRef(cm.GetOwnerReferences(), broker); appended {
		cm.SetOwnerReferences(refs)
		// only update if we actually appended an ownerRef
		_, err = r.KubeClient.CoreV1().ConfigMaps(broker.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
		return err
	}

	return nil
}

// TODO: copy paste!
func (r *Reconciler) createDataPlaneConfigMap(ctx context.Context, namespace string, options ...base.ConfigMapOption) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Env.DataPlaneConfigMapName,
			Namespace: namespace,
		},
		BinaryData: map[string][]byte{
			base.ConfigMapDataKey: []byte(""),
		},
	}

	for _, opt := range options {
		opt(cm)
	}

	return r.KubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
}
