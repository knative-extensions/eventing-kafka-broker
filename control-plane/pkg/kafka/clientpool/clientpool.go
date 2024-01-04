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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
	"knative.dev/pkg/logging"
)

var (
	clients *clientPool
)

func init() {
	cache := prober.NewLocalExpiringCache[clientKey, *client, struct{}](context.Background(), time.Minute*30)

	clients = &clientPool{
		Cache:                     cache,
		newSaramaClient:           sarama.NewClient,
		newClusterAdminFromClient: sarama.NewClusterAdminFromClient,
	}
}

// A comparable struct that includes the info we need to uniquely identify a kafka cluster admin
type clientKey struct {
	secretName       string
	secretNamespace  string
	bootstrapServers string
}

type client struct {
	lock sync.RWMutex
	sarama.Client
}

type clientPool struct {
	lock sync.RWMutex
	prober.Cache[clientKey, *client, struct{}]
	newSaramaClient           kafka.NewClientFunc // use this to mock the function for tests
	newClusterAdminFromClient kafka.NewClusterAdminFromClientFunc
}

type GetKafkaClientFunc func(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, error)
type GetKafkaClusterAdminFunc func(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, error)
type ReturnClientFunc func(bool)

func NilReturnClientFunc(bool) {}

func (cp *clientPool) get(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, ReturnClientFunc, error) {
	// (bootstrapServers, secret) uniquely identifies a sarama client config with the options we allow users to configure
	key := makeClusterAdminKey(bootstrapServers, secret)

	logger := logging.FromContext(ctx)

	logger.Debug("about to get connection from clientpool", zap.Any("key", key))

	cp.lock.RLock()

	// if a corresponding connection already exists, lets use it
	if val, ok := cp.Get(key); ok {
		logger.Debug("successfully got a client from the clientpool")
		cp.lock.RUnlock()

		val.lock.RLock()
		return val, func(shouldExpire bool) {
			val.lock.RUnlock()
			if shouldExpire {
				cp.Expire(key)
			}
		}, nil
	}
	logger.Debug("failed to get an existing client, going to create one")
	cp.lock.RUnlock()

	cp.lock.Lock()
	defer cp.lock.Unlock()

	// another connection may have been created before we get here, let's not upsert without checking as upsert closes existing connections and opens a new one
	// if a corresponding connection already exists, lets use it
	if val, ok := cp.Get(key); ok {
		logger.Debug("successfully got a client from the clientpool")

		val.lock.RLock()
		return val, func(shouldExpire bool) {
			val.lock.RUnlock()
			if shouldExpire {
				cp.Expire(key)
			}
		}, nil
	}

	saramaClient, err := cp.makeSaramaClient(bootstrapServers, secret)
	if err != nil {
		return nil, NilReturnClientFunc, err
	}

	val := &client{
		Client: saramaClient,
	}

	cp.UpsertStatus(key, val, struct{}{}, func(_ clientKey, value *client, _ struct{}) {
		value.lock.Lock()
		defer value.lock.Unlock()
		value.Close()
	})

	val.lock.RLock()
	return val, func(shouldExpire bool) {
		val.lock.RUnlock()
		if shouldExpire {
			cp.Expire(key)
		}
	}, nil
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
	if secret == nil {
		return key.secretName == "" && key.secretNamespace == ""
	}
	return key.secretName == secret.GetName() && key.secretNamespace == secret.GetNamespace()
}

func (key clientKey) getBootstrapServers() []string {
	return strings.Split(key.bootstrapServers, ",")
}

func (cp *clientPool) makeSaramaClient(bootstrapServers []string, secret *corev1.Secret) (sarama.Client, error) {
	config, err := kafka.GetSaramaConfig(security.NewSaramaSecurityOptionFromSecret(secret), kafka.DisableOffsetAutoCommitConfigOption)
	if err != nil {
		return nil, err
	}

	saramaClient, err := cp.newSaramaClient(bootstrapServers, config)
	if err != nil {
		return nil, err
	}

	return saramaClient, nil
}

func (cp *clientPool) updateConnectionsWithSecret(secret *corev1.Secret) error {
	for _, key := range cp.Keys() {
		if key.matchesSecret(secret) {
			newClient, err := cp.makeSaramaClient(key.getBootstrapServers(), secret)
			if err != nil {
				cp.Expire(key)
			} else {
				val := &client{
					Client: newClient,
				}
				cp.UpsertStatus(key, val, struct{}{}, func(_ clientKey, value *client, _ struct{}) {
					value.lock.Lock()
					defer value.lock.Unlock()
					value.Close()
				})
			}

		}

	}

	return nil
}

func (cp *clientPool) getClusterAdmin(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, ReturnClientFunc, error) {
	c, returnFunc, err := cp.get(ctx, bootstrapServers, secret)
	if err != nil {
		return nil, NilReturnClientFunc, err
	}
	ca, err := cp.newClusterAdminFromClient(c)
	if err != nil {
		returnFunc(true)
		return nil, NilReturnClientFunc, err
	}
	return ca, returnFunc, nil

}

func UpdateConnectionsWithSecret(secret *corev1.Secret) error {
	return clients.updateConnectionsWithSecret(secret)
}

// GetClusterAdmin returns a sarama.ClusterAdmin along with a ReturnClientFunc. The ReturnClientFunc MUST be called by the caller.
func GetClusterAdmin(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, ReturnClientFunc, error) {
	return clients.getClusterAdmin(ctx, bootstrapServers, secret)
}

// GetClient returns a sarama.Client along with a ReturnClientFunc. The ReturnClientFunc MUST be called by the caller.
func GetClient(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, ReturnClientFunc, error) {
	return clients.get(ctx, bootstrapServers, secret)
}
