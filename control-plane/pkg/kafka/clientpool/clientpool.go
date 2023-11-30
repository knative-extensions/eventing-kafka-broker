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
	"fmt"
	"sort"
	"strings"

	"github.com/IBM/sarama"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/lru"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	maxClients = 8
)

var (
	clients *clientPool
)

func init() {
	clients = newClusterAdminPool()
}

// A comparable struct that includes the info we need to uniquely identify a kafka cluster admin
type clusterAdminKey struct {
	secretUID        types.UID
	bootstrapServers string
}

type clientPool struct {
	clients lru.Cache
}

type GetKafkaClientFunc func(bootstrapServers []string, secret *corev1.Secret) (sarama.Client, error)
type GetKafkaClusterAdminFunc func(bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, error)

func newClusterAdminPool() *clientPool {
	return &clientPool{
		clients: *lru.NewWithEvictionFunc(maxClients, func(_ lru.Key, value interface{}) {
			if ca, ok := value.(sarama.Client); ok {
				ca.Close()
			}
		}),
	}
}

func (cap *clientPool) get(bootstrapServers []string, secret *corev1.Secret) (sarama.Client, error) {
	key := makeClusterAdminKey(bootstrapServers, secret)
	if val, ok := cap.clients.Get(key); ok {
		client, ok := val.(sarama.Client)
		if !ok {
			return nil, fmt.Errorf("a value which was not a sarama.Client was in the client pool")
		}
		return client, nil
	}

	config, err := kafka.GetSaramaConfig(security.NewSaramaSecurityOptionFromSecret(secret), kafka.DisableOffsetAutoCommitConfigOption)
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(bootstrapServers, config)
	if err != nil {
		return nil, err
	}

	cap.clients.Add(key, client)

	return client, nil
}

func makeClusterAdminKey(bootstrapServers []string, secret *corev1.Secret) clusterAdminKey {
	sort.SliceStable(bootstrapServers, func(i, j int) bool {
		return bootstrapServers[i] < bootstrapServers[j]
	})

	key := clusterAdminKey{
		bootstrapServers: strings.Join(bootstrapServers, ","),
	}

	if secret != nil {
		key.secretUID = secret.GetUID()
	}

	return key
}

func GetClusterAdmin(bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, error) {
	client, err := clients.get(bootstrapServers, secret)
	if err != nil {
		return nil, err
	}
	return sarama.NewClusterAdminFromClient(client)
}

func GetClient(bootstrapServers []string, secret *corev1.Secret) (sarama.Client, error) {
	return clients.get(bootstrapServers, secret)
}
