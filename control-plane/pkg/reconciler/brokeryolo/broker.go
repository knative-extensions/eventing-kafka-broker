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
	"os"
)

// TODO: read config-kafka-broker-data-plane in user namespace first, fallback to the one in knative-eventing
// TODO

type Reconciler struct {
	*base.Reconciler
	*config.Env

	Resolver *resolver.URIResolver

	ConfigMapLister corelisters.ConfigMapLister

	// NewKafkaClusterAdminClient creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc

	BootstrapServers string

	Enqueue prober.EnqueueFunc

	// BaseDataPlaneManifest is a Manifestival manifest that has the resources that need to be created in the user namespace.
	BaseDataPlaneManifest mf.Manifest

	Prober prober.Prober

	IPsLister prober.IPListerWithMapping
}

func setOwnerRef(broker *eventing.Broker) base.ConfigMapOption {
	return func(cm *corev1.ConfigMap) {
		existingRefs := cm.GetOwnerReferences()
		newRef := *metav1.NewControllerRef(broker, broker.GetGroupVersionKind())
		newRef.Controller = pointer.Bool(false)

		found := false
		for _, ref := range existingRefs {
			if ref.UID == newRef.UID {
				found = true

				// TODO: we can just replace it with the newRef!

				// API version can be changed with resource API version upgrade.
				// So, let's set that again... and, others as well
				ref.APIVersion = newRef.APIVersion
				ref.Kind = newRef.Kind
				ref.Name = newRef.Name
				ref.Controller = pointer.Bool(false)
				ref.BlockOwnerDeletion = pointer.Bool(true)

				break
			}
		}

		if !found {
			existingRefs = append(existingRefs, newRef)
		}

		cm.SetOwnerReferences(existingRefs)
	}
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

	manifest, err = manifest.Transform(appendNewOwnerRefsForBroker(manifest.Client, broker))
	if err != nil {
		return err
	}

	dispatcherImg, ok := os.LookupEnv("KNATIVE_KAFKA_DISPATCHER_IMAGE")
	if !ok {
		return fmt.Errorf("unable to find 'KNATIVE_KAFKA_DISPATCHER_IMAGE' env var in controller")
	}
	receiverImg, ok := os.LookupEnv("KNATIVE_KAFKA_RECEIVER_IMAGE")
	if !ok {
		return fmt.Errorf("unable to find 'KNATIVE_KAFKA_RECEIVER_IMAGE' env var in controller")
	}

	manifest, err = manifest.Transform(setImagesForDeployments(map[string]string{
		"${KNATIVE_KAFKA_DISPATCHER_IMAGE}": dispatcherImg,
		"${KNATIVE_KAFKA_RECEIVER_IMAGE}":   receiverImg,
	}))
	if err != nil {
		return err
	}

	err = manifest.Apply()
	if err != nil {
		return fmt.Errorf("unable to apply dataplane manifest. namespace: %s, owner: %v, error: %v", broker.Namespace, broker, err)
	}

	// Not necessary as we are creating this with owner ref
	//configmapInformer := configmapinformer.Get(ctx)
	//configmapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
	//	FilterFunc: controller.FilterWithNameAndNamespace(broker.Namespace, r.Env.DataPlaneConfigMapName),
	//	Handler: cache.ResourceEventHandlerFuncs{
	//		AddFunc: func(obj interface{}) {
	//			globalResync(obj)
	//		},
	//		DeleteFunc: func(obj interface{}) {
	//			globalResync(obj)
	//		},
	//	},
	//})

	//// 1. Create "data plane" based on broker.Namespace
	//if !r.IsReceiverRunning() || !r.IsDispatcherRunning() {
	//
	//	// TODO
	//	// return statusConditionManager.DataPlaneNotAvailable()
	//}

	// 2. Create broker.Reconciler pointing to the just created data plane
	// TODO: double RetryOnConflict
	if 1 == 1 {
		return br.ReconcileKind(ctx, broker)
	}
	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	logging.FromContext(ctx).Infof("YOLO FinalizeKind (wrapper)")
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, broker)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	logging.FromContext(ctx).Infof("YOLO finalizeKind")

	//// TODO remove ownerRef
	//// TODO: when ownerRef is removed, does the GC kick in?
	//
	//manifest, err := r.getManifestForNamespace(broker.Namespace)
	//if err != nil {
	//	return err
	//}
	//
	//manifest, err = r.removeOwnerRefsForBroker(manifest, broker)
	//if err != nil {
	//	return err
	//}
	//
	//err = manifest.Delete()
	//if err != nil {
	//	return err
	//}

	br := r.createReconcilerForBrokerInstance(ctx, broker)
	result := br.FinalizeKind(ctx, broker)

	r.IPsLister.Unregister(types.NamespacedName{Namespace: broker.Namespace, Name: broker.Name})

	return result
}

func (r *Reconciler) createReconcilerForBrokerInstance(ctx context.Context, broker *eventing.Broker) *brokerreconciler.Reconciler {

	//p = FakeProber{}

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

			// override
			// TODO: if we want to fall back to the global config for the general config in case it doesn't exist
			// in the user namespace, we will need a separate key for that
			SystemNamespace: broker.Namespace,

			// override
			// TODO: if we want to fall back to the global config for the dataplane configmap in case it doesn't exist
			// in the user namespace, we will need a separate key for that
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

	// TODO: move somewhere else
	// TODO: KNATIVE_KAFKA_RECEIVER_IMAGE crap!

	return manifest, nil
}

func appendNewOwnerRefsForBroker(client mf.Client, broker *eventing.Broker) mf.Transformer {
	// TODO: copy paste!
	return func(resource *unstructured.Unstructured) error {
		existingRefs, err := getExistingOwnerRefs(client, resource)
		if err != nil {
			return err
		}

		newRef := *metav1.NewControllerRef(broker, broker.GetGroupVersionKind())
		newRef.Controller = pointer.Bool(false)

		found := false
		for _, ref := range existingRefs {
			if ref.UID == newRef.UID {
				found = true

				// TODO: we can just replace it with the newRef!

				// API version can be changed with resource API version upgrade.
				// So, let's set that again... and, others as well
				ref.APIVersion = newRef.APIVersion
				ref.Kind = newRef.Kind
				ref.Name = newRef.Name
				ref.Controller = pointer.Bool(false)
				ref.BlockOwnerDeletion = pointer.Bool(true)

				break
			}
		}

		if !found {
			existingRefs = append(existingRefs, newRef)
		}

		resource.SetOwnerReferences(existingRefs)
		return nil
	}
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

//func (r *Reconciler) removeOwnerRefsForBroker(manifest mf.Manifest, broker *eventing.Broker) (mf.Manifest, error) {
//	var resources []unstructured.Unstructured
//	for _, res := range manifest.Resources() {
//		existingRefs, err := getExistingOwnerRefs(manifest.Client, res)
//		if err != nil {
//			//TODO: return proper error - with multierr
//			return mf.Manifest{}, err
//		}
//
//		var refs []metav1.OwnerReference
//
//		for _, ref := range existingRefs {
//			if ref.UID == broker.UID {
//				continue
//			}
//			refs = append(refs, ref)
//		}
//
//		res.SetOwnerReferences(refs)
//
//		resources = append(resources, res)
//	}
//
//	manifest, err := mf.ManifestFrom(manifestSource(resources), mf.UseClient(r.BaseDataPlaneManifest.Client))
//	if err != nil {
//		return mf.Manifest{}, fmt.Errorf("unable to transform base dataplane manifest with owner injection. owner: %v, error: %v", broker, err)
//	}
//	return manifest, nil
//}

func getExistingOwnerRefs(mfc mf.Client, res *unstructured.Unstructured) ([]metav1.OwnerReference, error) {
	retrieved, err := getResource(mfc, res)
	if err != nil {
		return nil, err
	}
	if retrieved == nil {
		return []metav1.OwnerReference{}, nil
	}
	existingRefs := retrieved.GetOwnerReferences()
	return existingRefs, nil
}

type ResourceArrayManifestSource struct {
	resources []unstructured.Unstructured
}

func (r *ResourceArrayManifestSource) Parse() ([]unstructured.Unstructured, error) {
	return r.resources, nil
}

func manifestSource(resources []unstructured.Unstructured) mf.Source {
	return &ResourceArrayManifestSource{resources: resources}
}

type FakeProber struct {
}

func (p FakeProber) Probe(ctx context.Context, addressable prober.Addressable, expected prober.Status) prober.Status {
	return prober.StatusReady
}

// get collects a full resource body (or `nil`) from a partial
// resource supplied in `spec`
func getResource(mfc mf.Client, spec *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if spec.GetName() == "" && spec.GetGenerateName() != "" {
		// expected to be created; never fetched
		return nil, nil
	}
	result, err := mfc.Get(spec)
	if err != nil {
		result = nil
		if errors.IsNotFound(err) {
			err = nil
		}
	}
	return result, err
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
		_, err = r.createDataPlaneConfigMap(ctx, broker.Namespace, setOwnerRef(broker))
		return err
	}

	// found, update
	setOwnerRef(broker)(cm)

	// TODO: check if already like this and do not update if so

	_, err = r.KubeClient.CoreV1().ConfigMaps(broker.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	return err
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
