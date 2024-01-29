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
)

type compositeProber struct {
	httpProber  prober
	httpsProber prober
}

// NewComposite creates a composite prober.
//
// It reports status changes using the provided EnqueueFunc.
func NewComposite(ctx context.Context, client httpClient, httpPort string, httpsPort string, IPsLister IPsLister, enqueue EnqueueFunc) (NewProber, error) {

	httpProber := NewAsync(ctx, client, httpPort, IPsLister, enqueue)
	httpsProber := NewAsync(ctx, client, httpsPort, IPsLister, enqueue)

	return &compositeProber{
		httpProber:  httpProber,
		httpsProber: httpsProber,
	}, nil
}

func (c *compositeProber) Probe(ctx context.Context, addressable ProberAddressable, expected Status) Status {
	var status Status
	for _, addr := range addressable.AddressStatus.Addresses {
		oldAddressable := proberAddressable{
			ResourceKey: addressable.ResourceKey,
			Address:     addr.URL.URL(),
		}
		if addr.URL.Scheme == "https" {
			status = c.httpsProber.probe(ctx, oldAddressable, expected)
		} else if addr.URL.Scheme == "http" {
			status = c.httpProber.probe(ctx, oldAddressable, expected)
		}
		if status != expected {
			return status
		}
	}
	return expected
}
