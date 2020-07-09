package base

import (
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"

	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
)

const (
	// log key of the data of the config map.
	BrokersTriggersDataLogKey = "brokerstriggers"

	// config map key of the brokers and triggers config map.
	ConfigMapDataKey = "data"

	// label for selecting dispatcher pods.
	DispatcherLabel = "kafka-broker-dispatcher"

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

func (r *Reconciler) GetBrokersTriggersConfigMap() (*corev1.ConfigMap, error) {
	return r.KubeClient.CoreV1().
		ConfigMaps(r.DataPlaneConfigMapNamespace).
		Get(r.DataPlaneConfigMapName, metav1.GetOptions{})
}

// getBrokersTriggers extracts brokers and triggers data from the given config map.
func (r *Reconciler) GetBrokersTriggers(logger *zap.Logger, brokersTriggersConfigMap *corev1.ConfigMap) (*coreconfig.Brokers, error) {

	brokersTriggersRaw, hasData := brokersTriggersConfigMap.BinaryData[ConfigMapDataKey]
	if !hasData || brokersTriggersRaw == nil {

		logger.Debug(
			fmt.Sprintf("Config map has no %s key, so start from scratch", ConfigMapDataKey),
		)

		return &coreconfig.Brokers{}, nil
	}

	brokersTriggers := &coreconfig.Brokers{}
	var err error

	logger.Debug(
		"Unmarshalling configmap",
		zap.String("format", r.DataPlaneConfigFormat),
	)

	// determine unmarshalling strategy
	switch r.DataPlaneConfigFormat {
	case Protobuf:
		err = proto.Unmarshal(brokersTriggersRaw, brokersTriggers)
	case Json:
		err = json.Unmarshal(brokersTriggersRaw, brokersTriggers)
	}
	if err != nil {

		logger.Warn("Failed to unmarshal config map", zap.Error(err))

		// let the caller decide if it want to continue or fail on an error.
		return &coreconfig.Brokers{}, fmt.Errorf("failed to unmarshal brokers and triggers: %w", err)
	}

	return brokersTriggers, nil
}

func (r *Reconciler) UpdateBrokersTriggersConfigMap(brokersTriggers *coreconfig.Brokers, configMap *corev1.ConfigMap) error {

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
		return fmt.Errorf("failed to update config map %s/%s: %w", configMap.Namespace, configMap.Name, err)
	}

	return nil
}

func (r *Reconciler) UpdateDispatcherPodsAnnotation(logger *zap.Logger, volumeGeneration uint64) error {

	namespace := r.SystemNamespace

	labelSelector := labels.SelectorFromSet(map[string]string{"app": DispatcherLabel})
	pods, errors := r.PodLister.Pods(namespace).List(labelSelector)
	if errors != nil {
		return fmt.Errorf("failed to list pods in namespace %s: %w", namespace, errors)
	}

	for _, p := range pods {

		logger.Debug(
			"Update dispatcher pod annotation",
			zap.String("pod", fmt.Sprintf("%s/%s", p.Namespace, p.Name)),
			zap.Uint64("volumeGeneration", volumeGeneration),
		)

		// do not update cache copy
		pod := p.DeepCopy()
		p = nil

		annotations := pod.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string, 1)
		}

		annotations[VolumeGenerationAnnotationKey] = fmt.Sprint(volumeGeneration)
		pod.SetAnnotations(annotations)

		if _, err := r.KubeClient.CoreV1().Pods(namespace).Update(pod); err != nil {

			errors = multierr.Append(errors, fmt.Errorf(
				"failed to update pod %s/%s: %w",
				pod.Namespace,
				pod.Name,
				err,
			))
		}
	}

	return errors
}
