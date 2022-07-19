package kafka

import (
	corev1 "k8s.io/api/core/v1"
)

const (
	NamespacedBrokerDataplaneLabelKey   = "eventing.knative.dev/namespaced"
	NamespacedBrokerDataplaneLabelValue = "true"
)

func NamespacedDataplaneLabelConfigmapOption(cm *corev1.ConfigMap) {
	if len(cm.Labels) == 0 {
		cm.Labels = make(map[string]string, 1)
	}
	cm.Labels[NamespacedBrokerDataplaneLabelKey] = NamespacedBrokerDataplaneLabelValue
}

func FilterWithNamespacedDataplaneLabel(obj interface{}) bool {
	if object, ok := obj.(corev1.ConfigMap); ok {
		if len(object.Labels) == 0 {
			return false
		}
		var v string
		var exists bool
		if v, exists = object.Labels[NamespacedBrokerClass]; !exists {
			return false
		}
		return v == NamespacedBrokerDataplaneLabelValue
	}
	return false
}
