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
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
	"knative.dev/pkg/logging"
)

type KafkaClientKey struct{}

var ctxKey = KafkaClientKey{}

func WithKafkaClientPool(ctx context.Context) context.Context {
	cache := prober.NewLocalExpiringCache[clientKey, *client, struct{}](ctx, time.Minute*30)

	clients := &ClientPool{
		cache:                     cache,
		newSaramaClient:           sarama.NewClient,
		newClusterAdminFromClient: sarama.NewClusterAdminFromClient,
	}

	return context.WithValue(ctx, ctxKey, clients)
}

// A comparable struct that includes the info we need to uniquely identify a kafka cluster admin
type clientKey struct {
	secretName       string
	secretNamespace  string
	bootstrapServers string
}

type ClientPool struct {
	cache                     prober.Cache[clientKey, *client, struct{}]
	newSaramaClient           kafka.NewClientFunc // use this to mock the function for tests
	newClusterAdminFromClient kafka.NewClusterAdminFromClientFunc
}

type GetKafkaClientFunc func(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, error)
type GetKafkaClusterAdminFunc func(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, error)

func (cp *ClientPool) GetClient(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.Client, error) {
	client, err := cp.getClient(ctx, bootstrapServers, secret)
	if err != nil {
		return nil, err
	}

	client.incrementCallers()
	return client, nil
}

func (cp *ClientPool) getClient(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (*client, error) {
	// (bootstrapServers, secret) uniquely identifies a sarama client config with the options we allow users to configure
	key := makeClusterAdminKey(bootstrapServers, secret)

	logger := logging.FromContext(ctx).With(zap.String("component", "clientpool")).With(zap.Any("key", key))

	logger.Debug("about to get connection from clientpool", zap.Any("key", key))

	// if a corresponding connection already exists, lets use it
	if val, ok := cp.cache.Get(key); ok && val.hasCorrectSecretVersion(secret) {
		logger.Debug("successfully got a client from the clientpool")
		return val, nil
	}
	logger.Debug("failed to get an existing client, going to create one")

	saramaClient, err := cp.makeSaramaClient(bootstrapServers, secret)
	if err != nil {
		return nil, err
	}

	val := &client{
		client: saramaClient,
		isFatalError: func(err error) bool {
			return err != nil && (strings.Contains(err.Error(), "broken pipe") || strings.Contains(err.Error(), "metadata is out of date"))
		},
		onFatalError: func(err error) {
			logger.Debug("handling fatal error, expiring key", zap.Error(err), zap.Any("key", key))
			cp.cache.Expire(key)
		},
		secret: secret,
	}

	cp.cache.UpsertStatus(key, val, struct{}{}, func(_ clientKey, value *client, _ struct{}) {
		// async: avoid blocking UpsertStatus if the key is present
		go func() {
			logger.Debugw("Closing client, waiting for callers to finish operations")

			// wait for all callers to finish
			value.callersWg.Wait()

			logger.Debug("Closing client")

			if err := value.client.Close(); err != nil && !errors.Is(err, sarama.ErrClosedClient) {
				logger.Errorw("Failed to close client", zap.Error(err))
			}
		}()
	})

	return val, nil
}

func (cp *ClientPool) GetClusterAdmin(ctx context.Context, bootstrapServers []string, secret *corev1.Secret) (sarama.ClusterAdmin, error) {
	c, err := cp.GetClient(ctx, bootstrapServers, secret)
	if err != nil {
		return nil, err
	}

	ca, err := clusterAdminFromClient(c, cp.newClusterAdminFromClient)
	if err != nil {
		c.Close()
		return nil, err
	}
	return ca, nil
}

func Get(ctx context.Context) *ClientPool {
	return ctx.Value(ctxKey).(*ClientPool)
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

func (cp *ClientPool) makeSaramaClient(bootstrapServers []string, secret *corev1.Secret) (sarama.Client, error) {
	secretOpt, err := security.NewSaramaSecurityOptionFromSecret(secret)
	if err != nil {
		return nil, err
	}

	config, err := kafka.GetSaramaConfig(secretOpt, kafka.DisableOffsetAutoCommitConfigOption)
	if err != nil {
		return nil, err
	}

	saramaClient, err := cp.newSaramaClient(bootstrapServers, config)
	if err != nil {
		return nil, err
	}

	return saramaClient, nil
}
