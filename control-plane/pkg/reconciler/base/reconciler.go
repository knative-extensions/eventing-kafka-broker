/*
 * Copyright 2020 The Knative Authors
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

package base

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/tracker"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	// log key of the data of the config map.
	ContractLogKey = "contract"

	// config map key of the contract config map.
	ConfigMapDataKey = "data"

	// label for selecting dispatcher pods.
	BrokerDispatcherLabel = "kafka-broker-dispatcher"
	// label for selecting receiver pods.
	BrokerReceiverLabel = "kafka-broker-receiver"
	// label for selecting receiver pods.
	SinkReceiverLabel = "kafka-sink-receiver"

	// volume generation annotation data plane pods.
	VolumeGenerationAnnotationKey = "volumeGeneration"

	Protobuf = "protobuf"
	Json     = "json"
)

// Base reconciler for broker and trigger reconciler.
// It contains common logic for both trigger and broker reconciler.
type Reconciler struct {
	KubeClient   kubernetes.Interface
	PodLister    corelisters.PodLister
	SecretLister corelisters.SecretLister

	SecretTracker    tracker.Interface
	ConfigMapTracker tracker.Interface

	DataPlaneConfigMapNamespace string
	DataPlaneConfigMapName      string
	DataPlaneConfigFormat       string
	SystemNamespace             string

	DispatcherLabel string
	ReceiverLabel   string
}

func (r *Reconciler) IsReceiverRunning() bool {
	pods, err := r.PodLister.List(r.receiverSelector())
	return err == nil && len(pods) > 0 && isAtLeastOneRunning(pods)
}

func (r *Reconciler) IsDispatcherRunning() bool {
	pods, err := r.PodLister.List(r.dispatcherSelector())
	return err == nil && len(pods) > 0 && isAtLeastOneRunning(pods)
}

func isAtLeastOneRunning(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodRunning {
			return true
		}
	}
	return false
}

func (r *Reconciler) GetOrCreateDataPlaneConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {

	cm, err := r.KubeClient.CoreV1().
		ConfigMaps(r.DataPlaneConfigMapNamespace).
		Get(ctx, r.DataPlaneConfigMapName, metav1.GetOptions{})

	if apierrors.IsNotFound(err) {
		cm, err = r.createDataPlaneConfigMap(ctx)
	}

	return cm, err
}

func (r *Reconciler) createDataPlaneConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.DataPlaneConfigMapName,
			Namespace: r.DataPlaneConfigMapNamespace,
		},
		BinaryData: map[string][]byte{
			ConfigMapDataKey: []byte(""),
		},
	}
	return r.KubeClient.CoreV1().ConfigMaps(r.DataPlaneConfigMapNamespace).Create(ctx, cm, metav1.CreateOptions{})
}

// GetDataPlaneConfigMapData extracts contract from the given config map.
func (r *Reconciler) GetDataPlaneConfigMapData(logger *zap.Logger, dataPlaneConfigMap *corev1.ConfigMap) (*contract.Contract, error) {
	return GetDataPlaneConfigMapData(logger, dataPlaneConfigMap, r.DataPlaneConfigFormat)
}

func GetDataPlaneConfigMapData(logger *zap.Logger, dataPlaneConfigMap *corev1.ConfigMap, format string) (*contract.Contract, error) {

	dataPlaneDataRaw, hasData := dataPlaneConfigMap.BinaryData[ConfigMapDataKey]
	if !hasData || dataPlaneDataRaw == nil {

		logger.Debug(
			fmt.Sprintf("Config map has no %s key, so start from scratch", ConfigMapDataKey),
		)

		return &contract.Contract{}, nil
	}

	if string(dataPlaneDataRaw) == "" {
		return &contract.Contract{}, nil
	}

	ct := &contract.Contract{}
	var err error

	logger.Debug(
		"Unmarshalling configmap",
		zap.String("format", format),
	)

	// determine unmarshalling strategy
	switch format {
	case Protobuf:
		err = proto.Unmarshal(dataPlaneDataRaw, ct)
	case Json:
		err = protojson.Unmarshal(dataPlaneDataRaw, ct)
	}
	if err != nil {

		logger.Warn("Failed to unmarshal config map", zap.Error(err))

		// let the caller decide if it want to continue or fail on an error.
		return ct, fmt.Errorf("failed to unmarshal contract: '%s' - %w", dataPlaneDataRaw, err)
	}

	return ct, nil
}

func (r *Reconciler) UpdateDataPlaneConfigMap(ctx context.Context, contract *contract.Contract, configMap *corev1.ConfigMap) error {

	var data []byte
	var err error
	switch r.DataPlaneConfigFormat {
	case Protobuf:
		data, err = proto.Marshal(contract)
	case Json:
		data, err = protojson.Marshal(contract)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal contract: %w", err)
	}

	// Update config map data. TODO is it safe to update this config map? do we need to copy it?
	configMap.BinaryData[ConfigMapDataKey] = data

	_, err = r.KubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(ctx, configMap, metav1.UpdateOptions{})
	if err != nil {
		// Return the same error, so that we can handle conflicting updates.
		return err
	}

	return nil
}

func (r *Reconciler) UpdateDispatcherPodsAnnotation(ctx context.Context, logger *zap.Logger, volumeGeneration uint64) error {

	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {

		labelSelector := r.dispatcherSelector()
		pods, errors := r.PodLister.Pods(r.SystemNamespace).List(labelSelector)
		if errors != nil {
			return fmt.Errorf("failed to list dispatcher pods in namespace %s: %w", r.SystemNamespace, errors)
		}

		return r.updatePodsAnnotation(ctx, logger, "dispatcher", volumeGeneration, pods)
	})
}

func (r *Reconciler) UpdateReceiverPodsAnnotation(ctx context.Context, logger *zap.Logger, volumeGeneration uint64) error {

	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {

		labelSelector := r.receiverSelector()
		pods, errors := r.PodLister.Pods(r.SystemNamespace).List(labelSelector)
		if errors != nil {
			return fmt.Errorf("failed to list receiver pods in namespace %s: %w", r.SystemNamespace, errors)
		}

		return r.updatePodsAnnotation(ctx, logger, "receiver", volumeGeneration, pods)
	})
}

func (r *Reconciler) updatePodsAnnotation(ctx context.Context, logger *zap.Logger, component string, volumeGeneration uint64, pods []*corev1.Pod) error {

	var errors error

	for _, pod := range pods {

		logger.Debug(
			"Update "+component+" pod annotation",
			zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
			zap.Uint64("volumeGeneration", volumeGeneration),
		)

		// do not update cache copy
		pod := pod.DeepCopy()

		annotations := pod.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string, 1)
		}

		annotations[VolumeGenerationAnnotationKey] = fmt.Sprint(volumeGeneration)
		pod.SetAnnotations(annotations)

		if _, err := r.KubeClient.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
			// Return the same error, so that we can handle conflicting updates.
			return err
		}
	}
	return errors
}

func (r *Reconciler) receiverSelector() labels.Selector {
	return labels.SelectorFromSet(map[string]string{"app": r.ReceiverLabel})
}

func (r *Reconciler) dispatcherSelector() labels.Selector {
	return labels.SelectorFromSet(map[string]string{"app": r.DispatcherLabel})
}

func (r *Reconciler) SecretProviderFunc() security.SecretProviderFunc {
	return security.DefaultSecretProviderFunc(r.SecretLister, r.KubeClient)
}

func (r *Reconciler) TrackSecret(secret *corev1.Secret, parent metav1.Object) error {
	if secret == nil {
		return nil
	}
	ref := tracker.Reference{
		// Do not use cm.APIVersion and cm.Kind since they might be empty when they've been pulled from a lister.
		APIVersion: "v1",
		Kind:       "Secret",
		Namespace:  secret.Namespace,
		Name:       secret.Name,
	}
	return r.SecretTracker.TrackReference(ref, parent)
}

func (r *Reconciler) TrackConfigMap(cm *corev1.ConfigMap, parent metav1.Object) error {
	if cm == nil {
		return nil
	}
	ref := tracker.Reference{
		// Do not use cm.APIVersion and cm.Kind since they might be empty when they've been pulled from a lister.
		APIVersion: "v1",
		Kind:       "ConfigMap",
		Namespace:  cm.Namespace,
		Name:       cm.Name,
	}
	return r.ConfigMapTracker.TrackReference(ref, parent)
}

func (r *Reconciler) OnDeleteObserver(obj interface{}) {
	if r.ConfigMapTracker != nil {
		r.ConfigMapTracker.OnDeletedObserver(obj)
	}
	if r.SecretTracker != nil {
		r.SecretTracker.OnDeletedObserver(obj)
	}
}
