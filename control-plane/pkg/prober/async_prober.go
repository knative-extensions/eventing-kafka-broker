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
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing/pkg/eventingtls"
	"knative.dev/pkg/logging"
)

var (
	cacheExpiryTime = time.Minute * 30
)

type IPsLister func(addressable proberAddressable) ([]string, error)

type asyncProber struct {
	client    httpClient
	enqueue   EnqueueFunc
	logger    *zap.Logger
	cache     Cache
	IPsLister IPsLister
	port      string
	clientMu  sync.Mutex
}

// NewAsync creates an async Prober.
//
// It reports status changes using the provided EnqueueFunc.
func NewAsync(ctx context.Context, client httpClient, port string, IPsLister IPsLister, enqueue EnqueueFunc) prober {
	logger := logging.FromContext(ctx).Desugar().
		With(zap.String("scope", "prober"))

	if len(port) > 0 && port[0] != ':' {
		port = ":" + port
	}
	return &asyncProber{
		client:    client,
		enqueue:   enqueue,
		logger:    logger,
		cache:     NewLocalExpiringCache(ctx, cacheExpiryTime),
		IPsLister: IPsLister,
		port:      port,
		clientMu:  sync.Mutex{},
	}
}

func NewAsyncWithTLS(ctx context.Context, port string, IPsLister IPsLister, enqueue EnqueueFunc, caCerts *string) (prober, error) {
	newClient, err := makeHttpClientWithTLS(caCerts)
	if err != nil {
		return nil, err
	}
	return NewAsync(ctx, newClient, port, IPsLister, enqueue), nil
}

func (a *asyncProber) probe(ctx context.Context, addressable proberAddressable, expected Status) Status {
	address := addressable.Address
	IPs, err := a.IPsLister(addressable)
	if err != nil {
		a.logger.Error("Failed to list IPs", zap.Error(err))
		return StatusUnknown
	}
	// Return `StatusNotReady` when there are no IPs.
	if len(IPs) == 0 {
		return StatusNotReady
	}

	// aggregatedCurrentKnownStatus keeps track of the current status in the cache excluding the expected status
	// since we just skip IPs that have the expected status. If all IPs have the expected status, this will be nil.
	//
	// If there is a status that is `StatusUnknown` the final status we want to return is `StatusUnknown`,
	// while we return `StatusNotReady` when the status is known and all probes returned `StatusNotReady`.
	var aggregatedCurrentKnownStatus *Status

	// wg keeps track of each request probe status.
	//
	// It goes to done once we have all probe request results regardless of whether they are coming from
	// the cache or from an actual request.
	var wg sync.WaitGroup
	wg.Add(len(IPs))

	// enqueueOnce allows requeuing the resource only once, when we have all probe request results.
	var enqueueOnce sync.Once

	for _, IP := range IPs {
		podUrl := *address
		podUrl.Host = IP + a.port
		address := podUrl.String()

		logger := a.logger.
			With(zap.String("IP", IP)).
			With(zap.String("address", address))

		currentStatus := a.cache.GetStatus(address)
		if currentStatus == expected {
			logger.Debug("Skip probing, status in cache is equal to the expected status", zap.String("status", currentStatus.String()))
			wg.Done()
			continue
		}
		// The lower the value of the Status, the higher it's precedence.
		// So, we update to the newer status only if it is higher precedence (and lower value)
		if aggregatedCurrentKnownStatus == nil || *aggregatedCurrentKnownStatus > currentStatus {
			aggregatedCurrentKnownStatus = &currentStatus
		}

		resourceKey := addressable.ResourceKey

		// We want to requeue the resource only once when we have all probe request
		// results.
		enqueueOnce.Do(func() {
			// Wait in a separate goroutine.
			go func() {
				// wait before requeue-ing, constant backoff strategy, so that we don't create a spinning
				// loop.
				time.Sleep(time.Second)
				// Wait for all the prober request results and then requeue the
				// resource.
				wg.Wait()

				a.enqueue(resourceKey)
			}()
		})

		go func() {
			defer wg.Done()
			// Probe the pod.
			a.clientMu.Lock()
			defer a.clientMu.Unlock()
			status := probe(ctx, a.client, logger, address)
			logger.Debug("Probe status", zap.Stringer("status", status))
			// Update the status in the cache.
			a.cache.UpsertStatus(address, status, resourceKey, a.enqueueArg)
		}()
	}

	if aggregatedCurrentKnownStatus == nil {
		// Every status is the expected value, return expected.
		return expected
	}
	// At least one status was not expected, return the aggregated status
	return *aggregatedCurrentKnownStatus
}

func (a *asyncProber) enqueueArg(_ string, arg interface{}) {
	a.enqueue(arg.(types.NamespacedName))
}

func (a *asyncProber) rotateRootCaCerts(caCerts *string) error {
	newClient, err := makeHttpClientWithTLS(caCerts)
	if err != nil {
		return err
	}

	a.clientMu.Lock()
	defer a.clientMu.Unlock()
	a.client.(*http.Client).CloseIdleConnections()
	a.client = newClient
	return nil
}

func makeHttpClientWithTLS(caCerts *string) (*http.Client, error) {
	var err error

	tlsClient := eventingtls.ClientConfig{CACerts: caCerts}

	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.TLSClientConfig, err = eventingtls.GetTLSClientConfig(tlsClient)
	if err != nil {
		return nil, err
	}

	return &http.Client{
		Transport: httpTransport,
	}, nil
}
