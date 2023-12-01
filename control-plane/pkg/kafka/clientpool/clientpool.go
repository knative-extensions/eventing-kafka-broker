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
	"sync"

	"github.com/IBM/sarama"
	"golang.org/x/sync/semaphore"
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
type clientKey struct {
	secretUID        types.UID
	bootstrapServers string
}

type client struct {
	rwMutex sync.RWMutex
	sarama.Client
}

type clientPool struct {
	clients      lru.Cache
	poolCapacity *semaphore.Weighted
	mutex        sync.Mutex
}

type ReturnClientFunc func()
type GetKafkaClientFunc func(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, ReturnClientFunc, error)
type GetKafkaClusterAdminFunc func(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, ReturnClientFunc, error)

func NilReturnClientFunc() {}

func newClusterAdminPool() *clientPool {
	sem := semaphore.NewWeighted(maxClients)
	return &clientPool{
		clients: *lru.NewWithEvictionFunc(maxClients, func(_ lru.Key, value interface{}) {
			if c, ok := value.(*client); ok {
				// make sure no one is currently using this client
				c.rwMutex.Lock()
				defer c.rwMutex.Unlock()
				c.Close()
				sem.Release(1)
			}
		}),
		poolCapacity: sem,
	}
}

func (cp *clientPool) get(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, ReturnClientFunc, error) {
	// LRU cache does not call the eviction function if a value is updated, so use a lock here to make sure we have no concurrency issues with ghost connections
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	// (bootstrapServers, secret) uniquely identifies a sarama client config with the options we allow users to configure
	key := makeClusterAdminKey(bootstrapServers, secret)

	// if a corresponding connection already exists, lets use it
	if val, ok := cp.clients.Get(key); ok {
		c, ok := val.(*client)
		if !ok {
			return nil, NilReturnClientFunc, fmt.Errorf("a value which was not a sarama.Client was in the client pool")
		}
		// clients are concurrency safe, but we use a read lock to allow multiple uses of the client while ensuring that the connection does not get closed while it is in use.
		c.rwMutex.RLock()
		return c, makeReturnClientFunc(c), nil
	}

	// if all connections are in use we don't want to be blocked as we need to signal to the pool to close the least recently used connection.
	if ok := cp.poolCapacity.TryAcquire(1); !ok {
		// This will remove the oldest client.
		cp.clients.RemoveOldest()
		// The eviction func in the cache will release 1 connection back to the pool after closing the connection.
		cp.poolCapacity.Acquire(ctx, 1)
	}

	config, err := kafka.GetSaramaConfig(security.NewSaramaSecurityOptionFromSecret(secret), kafka.DisableOffsetAutoCommitConfigOption)
	if err != nil {
		// we didn't actually make a connection, release the connection back to the pool
		cp.poolCapacity.Release(1)
		return nil, NilReturnClientFunc, err
	}

	saramaClient, err := sarama.NewClient(bootstrapServers, config)
	if err != nil {
		// we didn't actually make a connection, release the connection back to the pool
		cp.poolCapacity.Release(1)
		return nil, NilReturnClientFunc, err
	}

	c := &client{
		Client: saramaClient,
	}

	// clients are concurrency safe, but we use a read lock to allow multiple uses of the client while ensuring that the connection does not get closed while it is in use.
	c.rwMutex.RLock()
	cp.clients.Add(key, c)
	return c, makeReturnClientFunc(c), nil
}

// the return client func will release the read lock so that after all the in-use clients are returned, the client will be able to be closed if needed
func makeReturnClientFunc(c *client) ReturnClientFunc {
	return func() {
		c.rwMutex.RUnlock()
	}
}

func makeClusterAdminKey(bootstrapServers []string, secret *corev1.Secret) clientKey {
	sort.SliceStable(bootstrapServers, func(i, j int) bool {
		return bootstrapServers[i] < bootstrapServers[j]
	})

	key := clientKey{
		bootstrapServers: strings.Join(bootstrapServers, ","),
	}

	if secret != nil {
		key.secretUID = secret.GetUID()
	}

	return key
}

// GetClusterAdmin returns a sarama.ClusterAdmin along with a ReturnClientFunc. The ReturnClientFunc MUST be called by the caller.
func GetClusterAdmin(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, ReturnClientFunc, error) {
	c, returnFunc, err := clients.get(ctx, bootstrapServers, secret)
	if err != nil {
		return nil, NilReturnClientFunc, err
	}
	ca, err := sarama.NewClusterAdminFromClient(c)
	if err != nil {
		return nil, NilReturnClientFunc, err
	}
	return ca, returnFunc, nil
}

// GetClient returns a sarama.Client along with a ReturnClientFunc. The ReturnClientFunc MUST be called by the caller.
func GetClient(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, ReturnClientFunc, error) {
	return clients.get(ctx, bootstrapServers, secret)
}
