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
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/network"
)

// Addressable contains addressable resource data for the prober.
type Addressable struct {
	// Addressable address.
	Address *url.URL
	// Resource key.
	ResourceKey types.NamespacedName
}

// EnqueueFunc enqueues the given provided resource key.
type EnqueueFunc func(key types.NamespacedName)

// Prober probes an addressable resource.
type Prober interface {
	// Probe probes the provided Addressable resource and returns its Status.
	Probe(ctx context.Context, addressable Addressable, expected Status) Status
}

// Func type is an adapter to allow the use of
// ordinary functions as Prober. If f is a function
// with the appropriate signature, Func(f) is a
// Prober that calls f.
type Func func(ctx context.Context, addressable Addressable, expected Status) Status

// Probe implements the Prober interface for Func.
func (p Func) Probe(ctx context.Context, addressable Addressable, expected Status) Status {
	return p(ctx, addressable, expected)
}

// httpClient interface is an interface for an HTTP client.
type httpClient interface {
	Do(r *http.Request) (*http.Response, error)
}

func probe(ctx context.Context, client httpClient, logger *zap.Logger, address string) Status {
	logger.Debug("Sending probe request")

	r, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		logger.Error("Failed to create request", zap.Error(err))
		return StatusUnknown
	}
	r.Header.Add(network.ProbeHeaderName, network.ProbeHeaderValue)
	r.Header.Add(network.HashHeaderName, "probe")

	select {
	case <-ctx.Done():
		return StatusUnknown
	default:
	}

	response, err := client.Do(r)
	if err != nil {
		logger.Error("Failed probe", zap.Error(err))
		return StatusUnknown
	}

	if response.StatusCode != http.StatusOK {
		logger.Info("Resource not ready", zap.Int("statusCode", response.StatusCode))
		return StatusNotReady
	}

	return StatusReady
}

func IPsListerFromService(svc types.NamespacedName) IPsLister {
	return func(addressable Addressable) ([]string, error) {
		return []string{GetIPForService(svc)}, nil
	}
}

func GetIPForService(svc types.NamespacedName) string {
	return fmt.Sprintf("%s.%s.svc", svc.Name, svc.Namespace)
}

type IPListerWithMapping interface {
	Register(svc types.NamespacedName, ip string)
	Unregister(svc types.NamespacedName)
	List(addressable Addressable) ([]string, error)
}

type ipListerWithMapping struct {
	mx      sync.RWMutex
	mapping map[string]string
}

func NewIPListerWithMapping() *ipListerWithMapping {
	return &ipListerWithMapping{
		mapping: map[string]string{},
	}
}

func (m *ipListerWithMapping) Register(svc types.NamespacedName, ip string) {
	a := svc.String()

	m.mx.Lock()
	defer m.mx.Unlock()

	m.mapping[a] = ip
}

func (m *ipListerWithMapping) Unregister(svc types.NamespacedName) {
	a := svc.String()

	m.mx.Lock()
	defer m.mx.Unlock()

	delete(m.mapping, a)
}

func (m *ipListerWithMapping) List(addressable Addressable) ([]string, error) {
	a := addressable.ResourceKey.String()

	m.mx.RLock()
	defer m.mx.RUnlock()

	if ip, exists := m.mapping[a]; exists {
		return []string{ip}, nil
	}
	// TODO: shall we return an error?
	return nil, nil
}

func IdentityIPsLister() IPsLister {
	return func(addressable Addressable) ([]string, error) {
		return []string{addressable.Address.Host}, nil
	}
}
