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

var (
	emptyCaCerts = ""
)

type compositeProber struct {
	httpProber  Prober
	httpsProber Prober
}

// NewComposite creates a composite prober.
//
// It reports status changes using the provided EnqueueFunc.
func NewComposite(ctx context.Context, httpPort string, httpsPort string, IPsLister IPsLister, enqueue EnqueueFunc, caCerts *string) (NewProber, error) {
	httpProber := NewAsync(ctx, http.DefaultClient, httpPort, IPsLister, enqueue)
	httpsProber, err := NewAsyncWithTLS(ctx, httpsPort, IPsLister, enqueue, caCerts)
	if err != nil {
		return nil, err
	}
	return &compositeProber{
		httpProber:  httpProber,
		httpsProber: httpsProber,
	}, nil
}

// NewCompositeNoTLS creates a composite prober which will fail if it attempts to probe an https address
func NewCompositeNoTLS(ctx context.Context, httpPort string, IPsLister IPsLister, enqueue EnqueueFunc) (NewProber, error) {
	return NewComposite(ctx, httpPort, "443", IPsLister, enqueue, &emptyCaCerts)
}

func (c *compositeProber) Probe(ctx context.Context, addressable NewAddressable, expected Status) Status {
	var status Status
	for _, addr := range addressable.AddressStatus.Addresses {
		oldAddressable := Addressable{
			ResourceKey: addressable.ResourceKey,
			Address:     addr.URL.URL(),
		}
		if addr.URL.Scheme == "https" {
			status = c.httpsProber.Probe(ctx, oldAddressable, expected)
		} else if addr.URL.Scheme == "http" {
			status = c.httpProber.Probe(ctx, oldAddressable, expected)
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
