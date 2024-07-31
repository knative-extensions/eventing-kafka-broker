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
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/utils/pointer"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/tracker"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	// log key of the data of the config map.
	ContractLogKey = "contract"

	// config map key of the contract config map.
	ConfigMapDataKey = "data"

	// label for selecting broker dispatcher pods.
	BrokerDispatcherLabel = "kafka-broker-dispatcher"
	// label for selecting broker receiver pods.
	BrokerReceiverLabel = "kafka-broker-receiver"

	// label for selecting sink receiver pods.
	SinkReceiverLabel = "kafka-sink-receiver"

	// label for selecting source dipsatcher pods.
	SourceDispatcherLabel = "kafka-source-dispatcher"

	// label for selecting channel dispatcher pods.
	ChannelDispatcherLabel = "kafka-channel-dispatcher"
	// label for selecting channel receiver pods.
	ChannelReceiverLabel = "kafka-channel-receiver"

	// volume generation annotation data plane pods.
	VolumeGenerationAnnotationKey = "volumeGeneration"

	// config features update time annotation for data plane pods.
	ConfigFeaturesUpdatedAnnotationKey = "configFeaturesUpdatedAt"

	Protobuf = "protobuf"
	Json     = "json"
)

var (
	jsonUnmarshalOptions = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// Base reconciler for broker and trigger reconciler.
// It contains common logic for both trigger and broker reconciler.
type Reconciler struct {
	KubeClient   kubernetes.Interface
	PodLister    corelisters.PodLister
	SecretLister corelisters.SecretLister

	Tracker tracker.Interface

	DataPlaneConfigMapNamespace string
	ContractConfigMapName       string
	ContractConfigMapFormat     string
	DataPlaneNamespace          string

	DataPlaneConfigConfigMapName string

	DispatcherLabel string
	ReceiverLabel   string

	DataPlaneConfigMapTransformer ConfigMapOption
}

func (r *Reconciler) IsReceiverRunning() bool {
	pods, err := r.PodLister.Pods(r.DataPlaneNamespace).List(r.ReceiverSelector())
	return err == nil && len(pods) > 0 && isAtLeastOneRunning(pods)
}

func (r *Reconciler) IsDispatcherRunning() bool {
	pods, err := r.PodLister.Pods(r.DataPlaneNamespace).List(r.dispatcherSelector())
	return err == nil && len(pods) > 0 && isAtLeastOneRunning(pods)
}

func isAtLeastOneRunning(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if isReady(pod) {
			return true
		}
	}
	return false
}

func isReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

type ConfigMapOption func(cm *corev1.ConfigMap)

func NoopConfigmapOption(cm *corev1.ConfigMap) {}

func (r *Reconciler) GetOrCreateDataPlaneConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {

	cm, err := r.KubeClient.CoreV1().
		ConfigMaps(r.DataPlaneConfigMapNamespace).
		Get(ctx, r.ContractConfigMapName, metav1.GetOptions{})

	if apierrors.IsNotFound(err) {
		cm, err = r.createDataPlaneConfigMap(ctx)
	}

	if r.DataPlaneConfigMapTransformer != nil {
		r.DataPlaneConfigMapTransformer(cm)
	}

	return cm, err
}

func (r *Reconciler) createDataPlaneConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.ContractConfigMapName,
			Namespace: r.DataPlaneConfigMapNamespace,
		},
		BinaryData: map[string][]byte{
			ConfigMapDataKey: []byte(""),
		},
	}

	if r.DataPlaneConfigMapTransformer != nil {
		r.DataPlaneConfigMapTransformer(cm)
	}

	return r.KubeClient.CoreV1().ConfigMaps(r.DataPlaneConfigMapNamespace).Create(ctx, cm, metav1.CreateOptions{})
}

func PodOwnerReference(p *corev1.Pod) ConfigMapOption {
	expected := metav1.OwnerReference{
		APIVersion:         corev1.SchemeGroupVersion.String(),
		Kind:               "Pod",
		Name:               p.Name,
		UID:                p.UID,
		Controller:         pointer.Bool(true),
		BlockOwnerDeletion: pointer.Bool(true),
	}
	return func(cm *corev1.ConfigMap) {
		for _, or := range cm.OwnerReferences {
			if equality.Semantic.DeepDerivative(expected, or) {
				return
			}
		}
		cm.OwnerReferences = append(cm.OwnerReferences, expected)
	}
}

// GetDataPlaneConfigMapData extracts contract from the given config map.
func (r *Reconciler) GetDataPlaneConfigMapData(logger *zap.Logger, dataPlaneConfigMap *corev1.ConfigMap) (*contract.Contract, error) {
	return GetDataPlaneConfigMapData(logger, dataPlaneConfigMap, r.ContractConfigMapFormat)
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
		err = jsonUnmarshalOptions.Unmarshal(dataPlaneDataRaw, ct)
	}
	if err != nil {

		// Because of https://github.com/golang/protobuf/issues/1314, we cannot use the error message in the unit tests.
		// Also, the error here should only happen when there is an implementation issue as the configmap is only
		// to be touched by the controller.
		// Thus, let's not have the error message in the returned error. Instead, let's just print it.

		logger.Error("Failed to unmarshal contract", zap.Any("content", dataPlaneDataRaw), zap.Error(err))

		// let the caller decide if it want to continue or fail on an error.
		return ct, fmt.Errorf("failed to unmarshal contract: '%s'", dataPlaneDataRaw)
	}

	return ct, nil
}

func CompareSemanticEqual(ctx context.Context, ct *contract.Contract, existing *corev1.ConfigMap, format string) bool {
	existingCt, err := GetDataPlaneConfigMapData(logging.FromContext(ctx).Desugar(), existing, format)
	if existingCt != nil && err == nil {
		return contract.SemanticEqual(existingCt, ct)
	}
	return false
}

func (r *Reconciler) UpdateDataPlaneConfigMap(ctx context.Context, contract *contract.Contract, configMap *corev1.ConfigMap) error {
	if CompareSemanticEqual(ctx, contract, configMap, r.ContractConfigMapFormat) {
		return nil
	}

	// Resource changed, increment contract generation.
	coreconfig.IncrementContractGeneration(contract)

	var data []byte
	var err error
	switch r.ContractConfigMapFormat {
	case Protobuf:
		data, err = proto.Marshal(contract)
	case Json:
		data, err = protojson.Marshal(contract)
	default:
		return fmt.Errorf("unknown contract format %s", r.ContractConfigMapFormat)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal contract: %w", err)
	}

	// Update config map data.
	if configMap.BinaryData == nil {
		configMap.BinaryData = make(map[string][]byte, 1)
	}
	configMap.BinaryData[ConfigMapDataKey] = data

	_, err = r.KubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(ctx, configMap, metav1.UpdateOptions{})
	if err != nil {
		// Return the same error, so that we can handle conflicting updates.
		return err
	}

	return nil
}

func (r *Reconciler) UpdateDispatcherPodsContractGenerationAnnotation(ctx context.Context, logger *zap.Logger, volumeGeneration uint64) error {
	pods, errors := r.PodLister.Pods(r.DataPlaneNamespace).List(r.dispatcherSelector())
	if errors != nil {
		return fmt.Errorf("failed to list dispatcher pods in namespace %s: %w", r.DataPlaneNamespace, errors)
	}
	return r.UpdatePodsAnnotation(ctx, logger, "dispatcher", VolumeGenerationAnnotationKey, fmt.Sprint(volumeGeneration), pods)
}

func (r *Reconciler) UpdateReceiverPodsContractGenerationAnnotation(ctx context.Context, logger *zap.Logger, volumeGeneration uint64) error {
	pods, errors := r.PodLister.Pods(r.DataPlaneNamespace).List(r.ReceiverSelector())
	if errors != nil {
		return fmt.Errorf("failed to list receiver pods in namespace %s: %w", r.DataPlaneNamespace, errors)
	}
	return r.UpdatePodsAnnotation(ctx, logger, "receiver", VolumeGenerationAnnotationKey, fmt.Sprint(volumeGeneration), pods)
}

func (r *Reconciler) UpdateReceiverConfigFeaturesUpdatedAnnotation(ctx context.Context, logger *zap.Logger) error {
	pods, err := r.PodLister.Pods(r.DataPlaneNamespace).List(r.ReceiverSelector())
	if err != nil {
		return fmt.Errorf("failed to list receiver pods in namespace %s: %s", r.DataPlaneNamespace, err)
	}
	return r.UpdatePodsAnnotation(ctx, logger, "receiver", ConfigFeaturesUpdatedAnnotationKey, time.Now().String(), pods)
}

func (r *Reconciler) UpdatePodsAnnotation(ctx context.Context, logger *zap.Logger, component, annotationKey, annotationValue string, pods []*corev1.Pod) error {

	var errors error

	for _, pod := range pods {

		logger.Debug(
			"Update "+component+" pod annotation",
			zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
			zap.String(annotationKey, annotationValue),
		)

		// do not update cache copy
		pod := pod.DeepCopy()

		annotations := pod.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string, 1)
		}

		// Check whether pod's annotation is the expected one.
		if v, ok := annotations[annotationKey]; ok {
			if v == annotationValue {
				logger.Debug(component + " pod annotation already up to date")
				// annotation is already correct.
				continue
			}
		}

		annotations[annotationKey] = annotationValue
		pod.SetAnnotations(annotations)

		if _, err := r.KubeClient.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
			// Return the same error, so that we can handle conflicting updates.
			return err
		}
	}
	return errors
}

func (r *Reconciler) ReceiverSelector() labels.Selector {
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
	return r.Tracker.TrackReference(ref, parent)
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
	return r.Tracker.TrackReference(ref, parent)
}

func (r *Reconciler) OnDeleteObserver(obj interface{}) {
	if r.Tracker != nil {
		r.Tracker.OnDeletedObserver(obj)
	}
	if r.Tracker != nil {
		r.Tracker.OnDeletedObserver(obj)
	}
}

func (r *Reconciler) DeleteResource(ctx context.Context, logger *zap.Logger, uuid types.UID, ct *contract.Contract, contractConfigMap *corev1.ConfigMap) error {
	resourceIndex := coreconfig.FindResource(ct, uuid)
	if resourceIndex != coreconfig.NoResource {
		coreconfig.DeleteResource(ct, resourceIndex)

		logger.Debug("Resource deleted", zap.Int("index", resourceIndex))

		// Update the configuration map with the new contract data.
		if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
			return err
		}
		logger.Debug("Contract config map updated")
	}
	return nil
}
