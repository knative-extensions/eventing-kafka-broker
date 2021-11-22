/*
 * Copyright 2021 The Knative Authors
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

package prober

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	"knative.dev/pkg/logging"
)

type asyncProber struct {
	client    httpClient
	enqueue   EnqueueFunc
	logger    *zap.Logger
	cache     Cache
	podLister func() ([]*corev1.Pod, error)
	port      string
}

// NewAsync creates an async Prober.
//
// It reports status changes using the provided EnqueueFunc.
func NewAsync(ctx context.Context, client httpClient, port string, podsLabelsSelector labels.Selector, enqueue EnqueueFunc) Prober {
	logger := logging.FromContext(ctx).Desugar().
		With(zap.String("scope", "prober"))

	if len(port) == 0 {
		logger.Fatal("Port is required")
	}
	podLister := podinformer.Get(ctx).Lister()
	return &asyncProber{
		client:    client,
		enqueue:   enqueue,
		logger:    logger,
		cache:     NewLocalExpiringCache(ctx, 30*time.Minute),
		podLister: func() ([]*corev1.Pod, error) { return podLister.List(podsLabelsSelector) },
		port:      port,
	}
}

func (a *asyncProber) Probe(ctx context.Context, addressable Addressable) Status {
	address := addressable.Address
	pods, err := a.podLister()
	if err != nil {
		a.logger.Error("Failed to list pods", zap.Error(err))
		return StatusUnknown
	}
	// Return `StatusNotReady` when there are no pods.
	if len(pods) == 0 {
		return StatusNotReady
	}

	// aggregatedCurrentKnownStatus keeps track of the current status in the cache excluding `StatusReady`
	// since we just skip pods that have `StatusReady`.
	//
	// If there is a status that is `StatusUnknown` the final status  we want to return is `StatusUnknown`,
	// while we return `StatusNotReady` when the status is known and all probes returned `StatusNotReady`.
	var aggregatedCurrentKnownStatus *Status

	// wg keeps track of each request probe status.
	//
	// It goes to done once we have all probe request results regardless of whether they are coming from
	// the cache or from an actual request.
	var wg sync.WaitGroup
	wg.Add(len(pods))

	// enqueueOnce allows requeuing the resource only once, when we have all probe request results.
	var enqueueOnce sync.Once

	for _, p := range pods {
		podUrl := *address
		podUrl.Host = p.Status.PodIP + ":" + a.port
		address := podUrl.String()

		logger := a.logger.
			With(zap.String("pod.metadata.name", p.Name)).
			With(zap.String("address", address))

		currentStatus := a.cache.GetStatus(address)
		if currentStatus == StatusReady {
			logger.Debug("Skip probing", zap.String("status", "ready"))
			wg.Done()
			continue
		}
		if aggregatedCurrentKnownStatus == nil || *aggregatedCurrentKnownStatus > currentStatus {
			aggregatedCurrentKnownStatus = &currentStatus
		}

		resourceKey := addressable.ResourceKey

		// We want to requeue the resource only once when we have all probe request
		// results.
		enqueueOnce.Do(func() {
			// Wait in a separate gorouting.
			go func() {
				// Wait for all the prober request results and then requeue the
				// resource.
				wg.Wait()
				a.enqueue(resourceKey)
			}()
		})

		go func() {
			defer wg.Done()
			// Probe the pod.
			status := probe(ctx, a.client, logger, address)
			logger.Debug("Probe status", zap.Stringer("status", status))
			// Update the status in the cache.
			a.cache.UpsertStatus(address, status, resourceKey, a.enqueueArg)
		}()
	}

	if aggregatedCurrentKnownStatus == nil {
		// Every status is ready, return ready.
		return StatusReady
	}
	return *aggregatedCurrentKnownStatus
}

func (a *asyncProber) enqueueArg(_ string, arg interface{}) {
	a.enqueue(arg.(types.NamespacedName))
}
