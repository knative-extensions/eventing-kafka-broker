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

package prober

import (
	"context"
	"net/http"
)

type compositeProber struct {
	httpProber  Prober
	httpsProber Prober
}

// NewComposite creates a composite prober.
//
// It reports status changes using the provided EnqueueFunc.
func NewComposite(ctx context.Context, port string, IPsLister IPsLister, enqueue EnqueueFunc, caCerts *string) (Prober, error) {
	httpProber := NewAsync(ctx, http.DefaultClient, port, IPsLister, enqueue)
	httpsProber, err := NewAsyncWithTLS(ctx, port, IPsLister, enqueue, caCerts)
	if err != nil {
		return nil, err
	}
	return &compositeProber{
		httpProber:  httpProber,
		httpsProber: httpsProber,
	}, nil
}

func (c *compositeProber) Probe(ctx context.Context, addressables []Addressable, expected Status) Status {
	var status Status
	for i := range addressables {
		addr := addressables[i : i+1]
		if addr[0].Address.Scheme == "https" {
			status = c.httpsProber.Probe(ctx, addr, expected)
		} else if addr[0].Address.Scheme == "http" {
			c.httpProber.Probe(ctx, addr, expected)
		}
		if status != expected {
			return status
		}
	}
	return expected
}

func (c *compositeProber) RotateRootCaCerts(caCerts *string) error {
	// we don't need to rotate the certs on the http prober as it isn't using them
	err := c.httpsProber.RotateRootCaCerts(caCerts)
	return err
}
