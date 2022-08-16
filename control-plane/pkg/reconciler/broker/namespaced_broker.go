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
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"

	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	rbaclisters "k8s.io/client-go/listers/rbac/v1"

	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

// TODO: 5.
// TODO: IPListerWithMapping struct can leak resources in case of a leader change
// TODO: We might need to create a TTL mechanism

type NamespacedReconciler struct {
	*base.Reconciler
	*config.Env

	Resolver *resolver.URIResolver

	ConfigMapLister          corelisters.ConfigMapLister
	ServiceAccountLister     corelisters.ServiceAccountLister
	ClusterRoleBindingLister rbaclisters.ClusterRoleBindingLister
	DeploymentLister         appslisters.DeploymentLister

	// NewKafkaClusterAdminClient creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc

	BootstrapServers string

	Prober prober.Prober

	IPsLister          prober.IPListerWithMapping
	ManifestivalClient mf.Client
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
		return fmt.Errorf("unable to apply dataplane manifest. namespace: %s, broker: %v, error: %w", broker.Namespace, broker, err)
	}

	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("Data plane reconciled")

	for _, res := range manifest.Resources() {
		logger.Debug("Resource reconciled",
			zap.String("gvk", res.GroupVersionKind().String()),
			zap.String("resource.name", res.GetName()),
			zap.String("resource.namespace", res.GetNamespace()),
		)
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
	var resources []unstructured.Unstructured

	additionalConfigMaps, err := r.configMapsFromSystemNamespace(broker)
	if err != nil {
		return mf.Manifest{}, err
	}
	resources = append(resources, additionalConfigMaps...)

	additionalDeployments, err := r.deploymentsFromSystemNamespace(broker)
	if err != nil {
		return mf.Manifest{}, err
	}
	resources = append(resources, additionalDeployments...)

	additionalServiceAccounts, err := r.serviceAccountsFromSystemNamespace(broker)
	if err != nil {
		return mf.Manifest{}, err
	}
	resources = append(resources, additionalServiceAccounts...)

	additionalRoleBindings, err := r.roleBindingsFromSystemNamespace(broker)
	if err != nil {
		return mf.Manifest{}, err
	}
	resources = append(resources, additionalRoleBindings...)

	manifest, err := mf.ManifestFrom(mf.Slice(resources), mf.UseClient(r.ManifestivalClient))
	if err != nil {
		return mf.Manifest{}, fmt.Errorf("unable to load dataplane manifest: %w", err)
	}

	manifest, err = manifest.Transform(mf.InjectNamespace(broker.Namespace))
	if err != nil {
		return mf.Manifest{}, fmt.Errorf("unable to transform base dataplane manifest with namespace injection. namespace: %s, error: %v", broker.Namespace, err)
	}

	return manifest.Transform(appendNewOwnerRefsToPersisted(manifest.Client, broker))
}

func (r *NamespacedReconciler) deploymentsFromSystemNamespace(broker *eventing.Broker) ([]unstructured.Unstructured, error) {
	deployments := []string{
		"kafka-broker-receiver",
		"kafka-broker-dispatcher",
	}
	resources := make([]unstructured.Unstructured, len(deployments))
	for _, name := range deployments {
		resource, err := r.createManifestFromSystemDeployment(broker, name)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (r *NamespacedReconciler) configMapsFromSystemNamespace(broker *eventing.Broker) ([]unstructured.Unstructured, error) {
	configMaps := []string{
		"config-kafka-broker-data-plane",
		"config-tracing",
		"kafka-config-logging",
	}
	resources := make([]unstructured.Unstructured, len(configMaps))
	for _, name := range configMaps {
		resource, err := r.createResourceFromSystemConfigMap(broker, name)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (r *NamespacedReconciler) createResourceFromSystemConfigMap(broker *eventing.Broker, name string) (unstructured.Unstructured, error) {
	sysCM, err := r.ConfigMapLister.ConfigMaps(r.SystemNamespace).Get(name)
	if err != nil {
		return unstructured.Unstructured{}, fmt.Errorf("failed to get ConfigMap %s/%s: %w", r.SystemNamespace, name, err)
	}

	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{Kind: "ConfigMap", APIVersion: corev1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   broker.GetNamespace(),
			Name:        sysCM.Name,
			Labels:      sysCM.Labels,
			Annotations: sysCM.Annotations,
		},
		Immutable:  sysCM.Immutable,
		Data:       sysCM.Data,
		BinaryData: sysCM.BinaryData,
	}
	return unstructuredFromObject(cm)
}

func (r *NamespacedReconciler) createManifestFromSystemDeployment(broker *eventing.Broker, name string) (unstructured.Unstructured, error) {
	sysDeployment, err := r.DeploymentLister.Deployments(r.SystemNamespace).Get(name)
	if err != nil {
		return unstructured.Unstructured{}, fmt.Errorf("failed to get Deployment %s/%s: %w", r.SystemNamespace, name, err)
	}

	cm := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{Kind: "Deployment", APIVersion: appsv1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   broker.GetNamespace(),
			Name:        sysDeployment.Name,
			Labels:      sysDeployment.Labels,
			Annotations: sysDeployment.Annotations,
		},
		Spec: sysDeployment.Spec,
	}
	return unstructuredFromObject(cm)
}

func (r *NamespacedReconciler) serviceAccountsFromSystemNamespace(broker *eventing.Broker) ([]unstructured.Unstructured, error) {
	serviceAccounts := []string{
		"knative-kafka-broker-data-plane",
	}
	resources := make([]unstructured.Unstructured, len(serviceAccounts))
	for _, name := range serviceAccounts {
		resource, err := r.createManifestFromSystemServiceAccount(broker, name)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (r *NamespacedReconciler) createManifestFromSystemServiceAccount(broker *eventing.Broker, name string) (unstructured.Unstructured, error) {
	sysServiceAccount, err := r.ServiceAccountLister.ServiceAccounts(r.SystemNamespace).Get(name)
	if err != nil {
		return unstructured.Unstructured{}, fmt.Errorf("failed to get ServiceAccount %s/%s: %w", r.SystemNamespace, name, err)
	}

	cm := &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{Kind: "ServiceAccount", APIVersion: corev1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   broker.GetNamespace(),
			Name:        sysServiceAccount.Name,
			Labels:      sysServiceAccount.Labels,
			Annotations: sysServiceAccount.Annotations,
		},
		Secrets:                      sysServiceAccount.Secrets,
		ImagePullSecrets:             sysServiceAccount.ImagePullSecrets,
		AutomountServiceAccountToken: sysServiceAccount.AutomountServiceAccountToken,
	}
	return unstructuredFromObject(cm)
}

func (r *NamespacedReconciler) roleBindingsFromSystemNamespace(broker *eventing.Broker) ([]unstructured.Unstructured, error) {
	clusterRoleBindings := []string{
		"knative-kafka-broker-data-plane",
	}
	resources := make([]unstructured.Unstructured, len(clusterRoleBindings))
	for _, name := range clusterRoleBindings {
		resource, err := r.createManifestFromClusterRoleBinding(broker, name)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (r *NamespacedReconciler) createManifestFromClusterRoleBinding(broker *eventing.Broker, name string) (unstructured.Unstructured, error) {
	sysClusterRoleBinding, err := r.ClusterRoleBindingLister.Get(name)
	if err != nil {
		return unstructured.Unstructured{}, fmt.Errorf("failed to get ClusterRoleBinding %s: %w", name, err)
	}

	cm := &rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{Kind: "RoleBinding", APIVersion: rbacv1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   broker.GetNamespace(),
			Name:        sysClusterRoleBinding.Name,
			Labels:      sysClusterRoleBinding.Labels,
			Annotations: sysClusterRoleBinding.Annotations,
		},
		Subjects: sysClusterRoleBinding.Subjects,
		RoleRef:  sysClusterRoleBinding.RoleRef,
	}
	return unstructuredFromObject(cm)
}

func unstructuredFromObject(obj runtime.Object) (unstructured.Unstructured, error) {
	unstr, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return unstructured.Unstructured{}, err
	}
	return unstructured.Unstructured{Object: unstr}, nil
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
