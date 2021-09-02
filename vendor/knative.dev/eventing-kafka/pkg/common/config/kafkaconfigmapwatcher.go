/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"context"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/constants"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
)

// This function type is for a shim so that we can pass our own logger to the Observer function
type LoggingObserver func(ctx context.Context, configMap *corev1.ConfigMap)

//
// Initialize The Specified Context With A ConfigMap Watcher
// Much Of This Function Is Taken From The knative.dev sharedmain Package
//
func InitializeKafkaConfigMapWatcher(ctx context.Context, watcher configmap.Watcher, logger *zap.SugaredLogger, handler LoggingObserver, namespace string) error {
	// Start The ConfigMap Watcher
	// Taken from knative.dev/pkg/injection/sharedmain/main.go::WatchObservabilityConfigOrDie
	if _, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(namespace).Get(ctx, constants.SettingsConfigMapName, metav1.GetOptions{}); err == nil {
		watcher.Watch(constants.SettingsConfigMapName, func(configmap *corev1.ConfigMap) { handler(ctx, configmap) })
	} else if !apierrors.IsNotFound(err) {
		logger.Error("Error reading ConfigMap "+constants.SettingsConfigMapName, zap.Error(err))
		return err
	}

	return nil
}
