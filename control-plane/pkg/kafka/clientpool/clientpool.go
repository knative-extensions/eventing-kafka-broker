/*
 * Copyright 2023 The Knative Authors
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

package clientpool

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/IBM/sarama"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
	"knative.dev/pkg/logging"
)

const (
	capacityPerClient = 8
)

var (
	clients *clientPool
)

func init() {
	cache, err := NewLRUCache[clientKey, sarama.Client]()
	if err != nil {
		panic("kafka client pool tried to initialize with invalid paramters")
	}
	clients = &clientPool{
		cachePool: cache,
	}
}

// A comparable struct that includes the info we need to uniquely identify a kafka cluster admin
type clientKey struct {
	secretName       string
	secretNamespace  string
	bootstrapServers string
}

type clientPool struct {
	*cachePool[clientKey, sarama.Client]
}

type ReturnClientFunc func()
type GetKafkaClientFunc func(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, ReturnClientFunc, error)
type GetKafkaClusterAdminFunc func(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, ReturnClientFunc, error)

func NilReturnClientFunc() {}

func (cp *clientPool) get(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, ReturnClientFunc, error) {
	// (bootstrapServers, secret) uniquely identifies a sarama client config with the options we allow users to configure
	key := makeClusterAdminKey(bootstrapServers, secret)

	logging.FromContext(ctx).Info("cali0707: about to get connection from clientpool")

	// if a corresponding connection already exists, lets use it
	if val, returnClient, ok := cp.Get(ctx, key); ok {
		// check that the value is still good, as there are still some write errors
		ca, err := sarama.NewClusterAdminFromClient(val)
		if err != nil {
			returnClient()
			return nil, NilReturnClientFunc, err
		}
		if _, err := ca.ListTopics(); err != nil {
			logging.FromContext(ctx).Info("failed to list topics, refreshing brokers and metadata")
			err := val.RefreshBrokers(bootstrapServers)
			if err != nil {
				returnClient()
				logging.FromContext(ctx).Info("failed to refresh brokers")
				return nil, NilReturnClientFunc, err
			}
			err = val.RefreshMetadata()
			if err != nil {
				returnClient()
				logging.FromContext(ctx).Info("failed to refresh metadata")
				return nil, NilReturnClientFunc, err
			}
		}
		return val, returnClient, nil
	}

	// create a new client in the client pool, and acquire capacity to start using it right away
	saramaClient, returnClient, _, err := cp.AddAndAcquire(ctx, key, func() (sarama.Client, error) {
		saramaClient, err := makeSaramaClient(bootstrapServers, secret)
		if err != nil {
			return nil, err
		}
		return saramaClient, nil
	}, capacityPerClient)
	if err != nil {
		returnClient()
		return nil, NilReturnClientFunc, fmt.Errorf("error creating a new client: %v", err)
	}

	return saramaClient, returnClient, nil
}

func makeClusterAdminKey(bootstrapServers []string, secret *corev1.Secret) clientKey {
	sort.SliceStable(bootstrapServers, func(i, j int) bool {
		return bootstrapServers[i] < bootstrapServers[j]
	})

	key := clientKey{
		bootstrapServers: strings.Join(bootstrapServers, ","),
	}

	if secret != nil {
		key.secretName = secret.GetName()
		key.secretNamespace = secret.GetNamespace()
	}

	return key
}

func (key clientKey) matchesSecret(secret *corev1.Secret) bool {
	return key.secretName == secret.GetName() && key.secretNamespace == secret.GetNamespace()
}

func (key clientKey) getBootstrapServers() []string {
	return strings.Split(key.bootstrapServers, ",")
}

func makeSaramaClient(bootstrapServers []string, secret *corev1.Secret) (sarama.Client, error) {
	config, err := kafka.GetSaramaConfig(security.NewSaramaSecurityOptionFromSecret(secret), kafka.DisableOffsetAutoCommitConfigOption)
	if err != nil {
		return nil, err
	}

	saramaClient, err := sarama.NewClient(bootstrapServers, config)
	if err != nil {
		return nil, err
	}

	return saramaClient, nil
}

func (cp *clientPool) updateConnectionsWithSecret(secret *corev1.Secret) error {
	cp.lock.Lock()
	defer cp.lock.Unlock()
	for _, key := range cp.Keys() {
		if key.matchesSecret(secret) {
			exists, err := cp.UpdateIfExists(key, func() (sarama.Client, error) {
				saramaClient, err := makeSaramaClient(key.getBootstrapServers(), secret)
				if err != nil {
					return nil, err
				}
				return saramaClient, nil
			}, capacityPerClient)

			if err != nil && exists {
				return fmt.Errorf("failed to update the sarama client in the clientpool after recreating the client with the new secret: %v", err)
			}
		}

	}

	return nil
}

func UpdateConnectionsWithSecret(secret *corev1.Secret) error {
	return clients.updateConnectionsWithSecret(secret)
}

// GetClusterAdmin returns a sarama.ClusterAdmin along with a ReturnClientFunc. The ReturnClientFunc MUST be called by the caller.
func GetClusterAdmin(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, ReturnClientFunc, error) {
	c, returnFunc, err := clients.get(ctx, bootstrapServers, secret)
	if err != nil {
		return nil, NilReturnClientFunc, err
	}
	ca, err := sarama.NewClusterAdminFromClient(c)
	if err != nil {
		returnFunc()
		return nil, NilReturnClientFunc, err
	}
	return ca, returnFunc, nil
}

// GetClient returns a sarama.Client along with a ReturnClientFunc. The ReturnClientFunc MUST be called by the caller.
func GetClient(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, ReturnClientFunc, error) {
	return clients.get(ctx, bootstrapServers, secret)
}
