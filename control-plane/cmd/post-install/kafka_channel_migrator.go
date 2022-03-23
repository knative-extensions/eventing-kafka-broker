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

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	kcs "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	"knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
	"knative.dev/pkg/system"
	"sigs.k8s.io/yaml"
)

type kafkaChannelMigrator struct {
	kcs kcs.Interface
	k8s kubernetes.Interface
}

const (
	DataPlaneReadinessCheckInterval = 1 * time.Second
	DataPlaneReadinessCheckTimeout  = 2 * time.Minute

	NewChannelDispatcherDeploymentName = "kafka-channel-dispatcher"
	NewChannelReceiverDeploymentName   = "kafka-channel-receiver"

	NewChannelDispatcherServiceName = "kafka-channel-dispatcher"

	OldConfigmapName = "config-kafka"

	NewConfigmapNameKey     = "CHANNEL_GENERAL_CONFIG_MAP_NAME"
	NewConfigmapNameDefault = "kafka-channel-config"
)

func (m *kafkaChannelMigrator) Migrate(ctx context.Context) error {
	logger := logging.FromContext(ctx)

	logger.Infof("Waiting %s for the new data plane to become ready before the migration.", DataPlaneReadinessCheckTimeout)

	// wait until the new data plane is ready
	err := m.waitForNewDataPlaneReady(ctx)
	if err != nil {
		return fmt.Errorf("error while waiting the new data plane to become ready %w", err)
	}

	logger.Infof("New data plane is ready, progressing with the migration")

	newDispatcherExternalName := network.GetServiceHostname(NewChannelDispatcherServiceName, system.Namespace())

	// Sample YAML for a consolidated KafkaChannel service:
	// apiVersion: v1
	//kind: Service
	//metadata:
	//  creationTimestamp: "2022-03-14T07:12:39Z"
	//  labels:
	//    messaging.knative.dev/role: kafka-channel
	// ...
	//  name: kafka-channel-kn-channel
	//  namespace: default
	//  ownerReferences:
	//  - apiVersion: messaging.knative.dev/v1beta1
	//    blockOwnerDeletion: true
	//    controller: true
	//    kind: KafkaChannel
	//    name: kafka-channel
	//    uid: 7e638b1f-3227-42e3-9dcc-fcb36c7c6b4a
	//  resourceVersion: "86280"
	//  uid: 55aeaff5-207a-49bc-af17-118c513eb640
	//spec:
	//  externalName: kafka-ch-dispatcher.knative-eventing.svc.cluster.local
	//  sessionAffinity: None
	//  type: ExternalName
	//status:
	//  loadBalancer: {}

	// KafkaChannel service (the one that is created per channel instance) in consolidated channel
	// only has the label "messaging.knative.dev/role: kafka-channel".
	// Get those services and redirect them to new dispatcher.
	kafkaChannelServiceLabels := map[string]string{
		// See https://github.com/knative-sandbox/eventing-kafka/blob/23602e84d707b8c106c93ed9b12737263488ad18/pkg/channel/consolidated/reconciler/controller/resources/service.go#L70
		"messaging.knative.dev/role": "kafka-channel",
	}

	// cont takes care of results pagination.
	// there might be more resources than a single rest call can return, so we need to think about the pagination.
	cont := ""
	first := true
	lsOpts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(kafkaChannelServiceLabels).String(), Continue: cont}
	for first || cont != "" {
		kafkaChannelServiceList, err := m.k8s.CoreV1().
			Services(corev1.NamespaceAll).
			List(ctx, lsOpts)
		if err != nil {
			return fmt.Errorf("failed to list KafkaChannel services: %w", err)
		}
		for _, svc := range kafkaChannelServiceList.Items {

			patch := []byte(fmt.Sprintf(`[{"op":"replace", "path": "/spec/externalName", "value": "%s"}]`, newDispatcherExternalName))

			_, err := m.k8s.CoreV1().
				Services(svc.Namespace).
				Patch(ctx, svc.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
			if err != nil {
				// just log and continue with other services
				logger.Errorf("error while patching KafkaChannel service %s/%s: %w", svc.Namespace, svc.Name, err)
			}
		}
		cont = kafkaChannelServiceList.Continue
		first = false
	}

	// consolidated configmap looks like this:
	//
	// apiVersion: v1
	// data:
	//   eventing-kafka: |
	//     kafka:
	//       brokers: my-cluster-kafka-bootstrap.kafka:9092
	//       authSecretNamespace: my-namespace
	//       authSecretName: my-secret
	//   version: 1.0.0
	// kind: ConfigMap
	// metadata:
	//   name: config-kafka
	//   namespace: knative-eventing

	// the new configmap should look like this:
	//
	// apiVersion: v1
	// kind: ConfigMap
	// metadata:
	//   name: kafka-channel-config
	//   namespace: knative-eventing
	//  data:
	//    bootstrap.servers: "my-cluster-kafka-bootstrap.kafka:9092"
	//    auth.secret.ref.namespace: my-ns
	//    auth.secret.ref.name: my-secret
	// TODO:
	//    default.topic.partitions: "10"
	//    default.topic.replication.factor: "3"

	// get the old configmap, extract these and set it in the new configmap:
	// OLD CM PATH                                   NEW CM PATH
	// ----------------------------                  ------------------------
	// eventing-kafka/kafka/brokers              --> bootstrap.servers
	// eventing-kafka/kafka/authSecretNamespace  --> auth.secret.ref.namespace
	// eventing-kafka/kafka/authSecretName       --> auth.secret.ref.name
	//
	oldcm, err := m.k8s.CoreV1().
		ConfigMaps(system.Namespace()).
		Get(ctx, OldConfigmapName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		// configmap will be missing if we did the migration already
		return fmt.Errorf("failed to get consolidated channel configmap for migration %s: %w", OldConfigmapName, err)
	}

	// Can't use this as it defaults auth settings in case they're blank
	// oldconfig, err := consolidatedutils.GetKafkaConfig(ctx, "", oldcm.Data, consolidatedsarama.LoadAuthConfig)

	oldconfig, err := getEventingKafkaConfig(oldcm.Data)
	if err != nil && !apierrors.IsNotFound(err) {
		// configmap will be missing if we did the migration already
		return fmt.Errorf("failed to build config from consolidated channel configmap for migration %s: %w", OldConfigmapName, err)
	}

	newConfigmapName := os.Getenv(NewConfigmapNameKey)
	if newConfigmapName == "" {
		newConfigmapName = NewConfigmapNameDefault
	}

	patches := []string{}

	patches = append(patches,
		fmt.Sprintf(`{"op":"replace", "path": "/data/bootstrap.servers", "value": "%s"}`, oldconfig.Kafka.Brokers),
		fmt.Sprintf(`{"op":"replace", "path": "/data/auth.secret.ref.namespace", "value": "%s"}`, oldconfig.Kafka.AuthSecretNamespace),
		fmt.Sprintf(`{"op":"replace", "path": "/data/auth.secret.ref.name", "value": "%s"}`, oldconfig.Kafka.AuthSecretName),
	)

	logger.Infof("Patching configmap %s with patch %s", newConfigmapName, patches)

	patch := []byte(fmt.Sprintf("[%s]", strings.Join(patches, ",")))

	_, err = m.k8s.CoreV1().
		ConfigMaps(system.Namespace()).
		Patch(ctx, newConfigmapName, types.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch new channel configmap for migration %s: %w", newConfigmapName, err)
	}
	return nil
}

func getEventingKafkaConfig(configMap map[string]string) (*config.EventingKafkaConfig, error) {
	if len(configMap) == 0 {
		return nil, fmt.Errorf("missing configuration")
	}

	// Unmarshal The Eventing-Kafka ConfigMap YAML Into A EventingKafkaSettings Struct
	eventingKafkaConfig := &config.EventingKafkaConfig{}
	err := yaml.Unmarshal([]byte(configMap[constants.EventingKafkaSettingsConfigKey]), &eventingKafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("ConfigMap's eventing-kafka value could not be converted to an EventingKafkaConfig struct: %s : %v", err, configMap[constants.EventingKafkaSettingsConfigKey])
	}

	return eventingKafkaConfig, nil
}

func (m *kafkaChannelMigrator) waitForNewDataPlaneReady(ctx context.Context) error {
	return wait.PollImmediate(DataPlaneReadinessCheckInterval, DataPlaneReadinessCheckTimeout, func() (bool, error) {
		ready, err := m.isDeploymentReady(ctx, system.Namespace(), NewChannelDispatcherDeploymentName)
		if err != nil {
			return false, err
		}
		if !ready {
			return false, nil
		}

		return m.isDeploymentReady(ctx, system.Namespace(), NewChannelReceiverDeploymentName)
	})
}

func (m *kafkaChannelMigrator) isDeploymentReady(ctx context.Context, namespace, name string) (bool, error) {
	deployment, err := m.k8s.
		AppsV1().
		Deployments(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// Return false as we are not done yet.
		// We swallow the error to keep on polling.
		// It should only happen if we wait for the auto-created resources, like default Broker.
		return false, nil
	} else if err != nil {
		// Return error to stop the polling.
		return false, err
	}

	ready := GetDeploymentCondition(deployment.Status, appsv1.DeploymentAvailable)
	return ready != nil && ready.Status == corev1.ConditionTrue, nil
}

func GetDeploymentCondition(status appsv1.DeploymentStatus, t appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for _, cond := range status.Conditions {
		if cond.Type == t {
			return &cond
		}
	}
	return nil
}
