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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
	"os"
	"strings"
	"time"

	"encoding/base64"

	"go.uber.org/zap"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	oldChannelUtils "knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
	kcs "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
	"knative.dev/pkg/system"
)

type kafkaChannelMigrator struct {
	kcs kcs.Interface
	k8s kubernetes.Interface
}

const (
	DataPlaneReadinessCheckInterval = 10 * time.Second
	DataPlaneReadinessCheckTimeout  = 10 * time.Minute

	NewChannelDispatcherDeploymentName = "kafka-channel-dispatcher"
	NewChannelReceiverDeploymentName   = "kafka-channel-receiver"

	NewChannelDispatcherServiceName = "kafka-channel-ingress"

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
		if err == wait.ErrWaitTimeout {
			logger.Infof("Channel data plane does not exist - ignore migration")
			return nil
		}
		return fmt.Errorf("error while waiting the new data plane to become ready %w", err)
	}

	logger.Infof("New data plane is ready, progressing with the migration")

	err = m.migrateChannelServices(ctx, logger)
	if err != nil {
		return err
	}

	err = m.migrateConfigmap(ctx, logger)
	if err != nil {
		return err
	}

	return nil
}

func (m *kafkaChannelMigrator) migrateChannelServices(ctx context.Context, logger *zap.SugaredLogger) error {
	newDispatcherExternalName := network.GetServiceHostname(NewChannelDispatcherServiceName, system.Namespace())

	logger.Infof("Starting migration of channel services to new dispatcher service: %s.", newDispatcherExternalName)

	// Sample YAML for a consolidated KafkaChannel service:
	// apiVersion: v1
	//kind: Service
	//metadata:
	//  labels:
	//    messaging.knative.dev/role: kafka-channel
	// ...
	//  name: kafka-channel-kn-channel
	//  namespace: default
	//  ownerReferences:
	//  - apiVersion: messaging.knative.dev/v1beta1
	//    ...
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

			logger.Infof("Patching service %s/%s with the patch: %s.", svc.Namespace, svc.Name, patch)

			_, err := m.k8s.CoreV1().
				Services(svc.Namespace).
				Patch(ctx, svc.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
			if err != nil {
				// return error to crash so that the job can retry
				return fmt.Errorf("error while patching KafkaChannel service %s/%s: %w", svc.Namespace, svc.Name, err)
			}
		}
		cont = kafkaChannelServiceList.Continue
		first = false
	}
	return nil
}

func (m *kafkaChannelMigrator) migrateConfigmap(ctx context.Context, logger *zap.SugaredLogger) error {
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
	// These 2 things don't have anything in old configmap. Defaults of them in the old channel are hardcoded.
	//    default.topic.partitions: "10"
	//    default.topic.replication.factor: "3"

	// get the old configmap, extract these and set it in the new configmap:
	// OLD CM PATH                                   NEW CM PATH
	// ----------------------------                  ------------------------
	// eventing-kafka/kafka/brokers              --> bootstrap.servers
	// eventing-kafka/kafka/authSecretNamespace  --> auth.secret.ref.namespace
	// eventing-kafka/kafka/authSecretName       --> auth.secret.ref.name
	//

	logger.Infof("Migrating configmap.")

	oldcm, err := m.k8s.CoreV1().
		ConfigMaps(system.Namespace()).
		Get(ctx, OldConfigmapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) || len(oldcm.Data) == 0 {
		logger.Infof("Old configmap %s is either missing or empty. Skipping the configmap migration", OldConfigmapName)
		return nil
	}

	if err != nil {
		// there's some other problem
		return fmt.Errorf("failed to get consolidated channel configmap for migration %s: %w", OldConfigmapName, err)
	}

	oldconfig, err := getEventingKafkaConfig(ctx, logger, oldcm.Data)
	if err != nil && !apierrors.IsNotFound(err) {
		// configmap will be missing if we did the migration already
		return fmt.Errorf("failed to build config from consolidated channel configmap for migration %s: %w", OldConfigmapName, err)
	}

	newConfigmapName := os.Getenv(NewConfigmapNameKey)
	if newConfigmapName == "" {
		newConfigmapName = NewConfigmapNameDefault
	}

	patches := []string{
		fmt.Sprintf(`{"op":"replace", "path": "/data/bootstrap.servers", "value": "%s"}`, oldconfig.EventingKafka.Kafka.Brokers),
	}
	if oldconfig.EventingKafka.Kafka.AuthSecretNamespace != "" {
		patches = append(patches, fmt.Sprintf(`{"op":"replace", "path": "/data/auth.secret.ref.namespace", "value": "%s"}`, oldconfig.EventingKafka.Kafka.AuthSecretNamespace))
	}
	if oldconfig.EventingKafka.Kafka.AuthSecretName != "" {
		patches = append(patches, fmt.Sprintf(`{"op":"replace", "path": "/data/auth.secret.ref.name", "value": "%s"}`, oldconfig.EventingKafka.Kafka.AuthSecretName))
	}

	logger.Infof("Patching configmap %s with patch %s", newConfigmapName, patches)

	patch := []byte(fmt.Sprintf("[%s]", strings.Join(patches, ",")))

	_, err = m.k8s.CoreV1().
		ConfigMaps(system.Namespace()).
		Patch(ctx, newConfigmapName, types.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch new channel configmap for migration %s: %w", newConfigmapName, err)
	}

	return m.migrateSecret(ctx, logger, oldconfig)
}

func (m *kafkaChannelMigrator) migrateSecret(ctx context.Context, logger *zap.SugaredLogger, oldconfig *oldChannelUtils.KafkaConfig) error {
	// Old secret doesn't require an explicit specification of the protocol.
	// Thus, we build the old config, see if TLS and SASL are enabled and set the protocol on the secret accordingly.
	// By this approach, we touch the secret but we don't need to do some on-the-fly config upgrade in the channel
	// reconciliation code.

	logger.Infof("Migrating auth secret.")

	secret, err := m.k8s.CoreV1().
		Secrets(oldconfig.EventingKafka.Kafka.AuthSecretNamespace).
		Get(ctx, oldconfig.EventingKafka.Kafka.AuthSecretName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("unable to find the old secret for migration %s/%s: %w", oldconfig.EventingKafka.Kafka.AuthSecretNamespace, oldconfig.EventingKafka.Kafka.AuthSecretName, err)
	}

	if _, ok := secret.Data[security.ProtocolKey]; ok {
		logger.Infof("secret already has %s=%s key defined. Skipping the secret migration", security.ProtocolKey, secret.Data[security.ProtocolKey])
		return nil
	}

	protocol := security.ProtocolPlaintext

	if oldconfig.EventingKafka.Sarama.Config.Net.SASL.Enable && oldconfig.EventingKafka.Sarama.Config.Net.TLS.Enable {
		protocol = security.ProtocolSASLSSL
	} else if oldconfig.EventingKafka.Sarama.Config.Net.SASL.Enable {
		protocol = security.ProtocolSASLPlaintext
	} else if oldconfig.EventingKafka.Sarama.Config.Net.TLS.Enable {
		protocol = security.ProtocolSSL
	}

	logger.Infof("Secret does not contain `%s`, patching it to contain the `%s=%s` to be explicit for coniguration", security.ProtocolKey, security.ProtocolKey, protocol)

	base64EncodedProtocol := make([]byte, base64.StdEncoding.EncodedLen(len(protocol)))
	base64.StdEncoding.Encode(base64EncodedProtocol, []byte(protocol))

	patches := []string{
		fmt.Sprintf(`{"op":"replace", "path": "/data/%s", "value": "%s"}`, security.ProtocolKey, base64EncodedProtocol),
	}

	logger.Infof("Patching secret %s/%s with patch %s", oldconfig.EventingKafka.Kafka.AuthSecretNamespace, oldconfig.EventingKafka.Kafka.AuthSecretName, patches)

	patch := []byte(fmt.Sprintf("[%s]", strings.Join(patches, ",")))

	_, err = m.k8s.CoreV1().
		Secrets(oldconfig.EventingKafka.Kafka.AuthSecretNamespace).
		Patch(ctx, oldconfig.EventingKafka.Kafka.AuthSecretName, types.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch secret for migration %s%s: %w", oldconfig.EventingKafka.Kafka.AuthSecretNamespace, oldconfig.EventingKafka.Kafka.AuthSecretName, err)
	}
	return nil
}

func getEventingKafkaConfig(ctx context.Context, logger *zap.SugaredLogger, configMap map[string]string) (*oldChannelUtils.KafkaConfig, error) {
	oldKafkaConfig, err := oldChannelUtils.GetKafkaConfig(ctx, "", configMap, kafkasarama.LoadAuthConfig)
	if err != nil {
		logger.Errorw("Error reading Kafka configuration", zap.Error(err))
		return nil, err
	}

	return oldKafkaConfig, nil
}

func (m *kafkaChannelMigrator) waitForNewDataPlaneReady(ctx context.Context) error {
	return wait.PollImmediate(DataPlaneReadinessCheckInterval, DataPlaneReadinessCheckTimeout, func() (bool, error) {
		ready, err := isDeploymentReady(ctx, m.k8s, system.Namespace(), NewChannelDispatcherDeploymentName)
		if err != nil {
			return false, err
		}
		if !ready {
			return false, nil
		}

		return isDeploymentReady(ctx, m.k8s, system.Namespace(), NewChannelReceiverDeploymentName)
	})
}

func GetDeploymentCondition(status appsv1.DeploymentStatus, t appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for _, cond := range status.Conditions {
		if cond.Type == t {
			return &cond
		}
	}
	return nil
}
