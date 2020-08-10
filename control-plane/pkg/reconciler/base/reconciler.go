package base

import (
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"

	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
)

const (
	// log key of the data of the config map.
	BrokersTriggersDataLogKey = "brokerstriggers"

	// config map key of the brokers and triggers config map.
	ConfigMapDataKey = "data"

	// label for selecting dispatcher pods.
	DispatcherLabel = "kafka-broker-dispatcher"
	// label for selecting receiver pods.
	ReceiverLabel = "kafka-broker-receiver"

	// volume generation annotation data plane pods.
	VolumeGenerationAnnotationKey = "volumeGeneration"

	Protobuf = "protobuf"
	Json     = "json"
)

// Base reconciler for broker and trigger reconciler.
// It contains common logic for both trigger and broker reconciler.
type Reconciler struct {
	KubeClient kubernetes.Interface
	PodLister  corelisters.PodLister

	DataPlaneConfigMapNamespace string
	DataPlaneConfigMapName      string
	DataPlaneConfigFormat       string
	SystemNamespace             string
}

func (r *Reconciler) GetOrCreateDataPlaneConfigMap() (*corev1.ConfigMap, error) {

	cm, err := r.KubeClient.CoreV1().
		ConfigMaps(r.DataPlaneConfigMapNamespace).
		Get(r.DataPlaneConfigMapName, metav1.GetOptions{})

	if apierrors.IsNotFound(err) {
		cm, err = r.createDataPlaneConfigMap()
	}

	return cm, err
}

func (r *Reconciler) createDataPlaneConfigMap() (*corev1.ConfigMap, error) {
	return r.KubeClient.CoreV1().ConfigMaps(r.DataPlaneConfigMapNamespace).Create(&corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.DataPlaneConfigMapName,
			Namespace: r.DataPlaneConfigMapNamespace,
		},
		BinaryData: map[string][]byte{
			ConfigMapDataKey: []byte(""),
		},
	})
}

// GetDataPlaneConfigMapData extracts brokers and triggers data from the given config map.
func (r *Reconciler) GetDataPlaneConfigMapData(logger *zap.Logger, dataPlaneConfigMap *corev1.ConfigMap) (*coreconfig.Brokers, error) {
	return GetDataPlaneConfigMapData(logger, dataPlaneConfigMap, r.DataPlaneConfigFormat)
}

func GetDataPlaneConfigMapData(logger *zap.Logger, dataPlaneConfigMap *corev1.ConfigMap, format string) (*coreconfig.Brokers, error) {

	dataPlaneDataRaw, hasData := dataPlaneConfigMap.BinaryData[ConfigMapDataKey]
	if !hasData || dataPlaneDataRaw == nil {

		logger.Debug(
			fmt.Sprintf("Config map has no %s key, so start from scratch", ConfigMapDataKey),
		)

		return &coreconfig.Brokers{}, nil
	}

	if string(dataPlaneDataRaw) == "" {
		return &coreconfig.Brokers{}, nil
	}

	brokersTriggers := &coreconfig.Brokers{}
	var err error

	logger.Debug(
		"Unmarshalling configmap",
		zap.String("format", format),
	)

	// determine unmarshalling strategy
	switch format {
	case Protobuf:
		err = proto.Unmarshal(dataPlaneDataRaw, brokersTriggers)
	case Json:
		err = json.Unmarshal(dataPlaneDataRaw, brokersTriggers)
	}
	if err != nil {

		logger.Warn("Failed to unmarshal config map", zap.Error(err))

		// let the caller decide if it want to continue or fail on an error.
		return &coreconfig.Brokers{}, fmt.Errorf("failed to unmarshal brokers and triggers: '%s' - %w", dataPlaneDataRaw, err)
	}

	return brokersTriggers, nil
}

func (r *Reconciler) UpdateDataPlaneConfigMap(brokersTriggers *coreconfig.Brokers, configMap *corev1.ConfigMap) error {

	var data []byte
	var err error
	switch r.DataPlaneConfigFormat {
	case Json:
		data, err = json.Marshal(brokersTriggers)
	case Protobuf:
		data, err = proto.Marshal(brokersTriggers)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal brokers and triggers: %w", err)
	}

	// Update config map data. TODO is it safe to update this config map? do we need to copy it?
	configMap.BinaryData[ConfigMapDataKey] = data

	_, err = r.KubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(configMap)
	if err != nil {
		// Return the same error, so that we can handle conflicting updates.
		return err
	}

	return nil
}

func (r *Reconciler) UpdateDispatcherPodsAnnotation(logger *zap.Logger, volumeGeneration uint64) error {

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {

		labelSelector := labels.SelectorFromSet(map[string]string{"app": DispatcherLabel})
		pods, errors := r.PodLister.Pods(r.SystemNamespace).List(labelSelector)
		if errors != nil {
			return fmt.Errorf("failed to list dispatcher pods in namespace %s: %w", r.SystemNamespace, errors)
		}

		return r.updatePodsAnnotation(logger, "dispatcher", volumeGeneration, pods)
	})
}

func (r *Reconciler) UpdateReceiverPodsAnnotation(logger *zap.Logger, volumeGeneration uint64) error {

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {

		labelSelector := labels.SelectorFromSet(map[string]string{"app": ReceiverLabel})
		pods, errors := r.PodLister.Pods(r.SystemNamespace).List(labelSelector)
		if errors != nil {
			return fmt.Errorf("failed to list receiver pods in namespace %s: %w", r.SystemNamespace, errors)
		}

		return r.updatePodsAnnotation(logger, "receiver", volumeGeneration, pods)
	})
}

func (r *Reconciler) updatePodsAnnotation(logger *zap.Logger, component string, volumeGeneration uint64, pods []*corev1.Pod) error {

	var errors error

	for _, pod := range pods {

		logger.Debug(
			"Update " + component + " pod annotation",
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

		if _, err := r.KubeClient.CoreV1().Pods(pod.Namespace).Update(pod); err != nil {
			// Return the same error, so that we can handle conflicting updates.
			return err
		}
	}
	return errors
}

func (r *Reconciler) HandleConflicts(f func() error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, f)
}
