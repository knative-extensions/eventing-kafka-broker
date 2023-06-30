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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"k8s.io/apimachinery/pkg/types"
)

func TestFuncProbe(t *testing.T) {

	calls := atomic.NewInt32(0)
	status := StatusReady
	p := Func(func(ctx context.Context, addressable Addressable, expected Status) Status {
		calls.Inc()
		return status
	})

	s := p.Probe(context.Background(), Addressable{}, status)

	require.Equal(t, status, s, s.String())
	require.Equal(t, int32(1), calls.Load())
}

func TestIPsListerFromService(t *testing.T) {
	tests := []struct {
		name    string
		svc     types.NamespacedName
		want    []string
		wantErr bool
	}{
		{
			name: "ok",
			svc: types.NamespacedName{
				Namespace: "ns",
				Name:      "name",
			},
			want:    []string{"name.ns.svc"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IPsListerFromService(tt.svc)(Addressable{})
			if tt.wantErr != (err != nil) {
				t.Errorf("Got err %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Error("(-want, +got)", diff)
			}
		})
	}
}
