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

package probertesting

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
)

func TestMockProber(t *testing.T) {
	tests := []struct {
		name        string
		status      prober.Status
		ctx         context.Context
		addressable prober.ProberAddressable
	}{
		{
			name:        "unknown",
			status:      prober.StatusUnknown,
			ctx:         context.Background(),
			addressable: prober.ProberAddressable{},
		},
		{
			name:        "ready",
			status:      prober.StatusReady,
			ctx:         context.Background(),
			addressable: prober.ProberAddressable{},
		},
		{
			name:        "notReady",
			status:      prober.StatusNotReady,
			ctx:         context.Background(),
			addressable: prober.ProberAddressable{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MockNewProber(tt.status).Probe(tt.ctx, tt.addressable, tt.status)
			if diff := cmp.Diff(tt.status, got); diff != "" {
				t.Error("(-want, got)", diff)
			}
		})
	}
}
