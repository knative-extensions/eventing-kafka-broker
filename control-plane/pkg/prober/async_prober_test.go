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
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	"knative.dev/pkg/network"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
)

func TestAsyncProber(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name                string
		pods                []*corev1.Pod
		podsLabelsSelector  labels.Selector
		addressable         Addressable
		responseStatusCode  int
		wantStatus          Status
		wantRequeueCountMin int
		wantRequestCountMin int
	}{
		{
			name:               "no pods",
			pods:               []*corev1.Pod{},
			podsLabelsSelector: labels.SelectorFromSet(map[string]string{"app": "p"}),
			addressable: Addressable{
				Address:     &url.URL{Scheme: "http", Path: "/b1/b1"},
				ResourceKey: types.NamespacedName{Namespace: "b1", Name: "b1"},
			},
			responseStatusCode:  http.StatusOK,
			wantStatus:          StatusNotReady,
			wantRequeueCountMin: 0,
		},
		{
			name: "single pod",
			pods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "ns",
						Name:      "p1",
						Labels:    map[string]string{"app": "p"},
					},
					Status: corev1.PodStatus{PodIP: "127.0.0.1"},
				},
			},
			podsLabelsSelector: labels.SelectorFromSet(map[string]string{"app": "p"}),
			addressable: Addressable{
				Address:     &url.URL{Scheme: "http", Path: "/b1/b1"},
				ResourceKey: types.NamespacedName{Namespace: "b1", Name: "b1"},
			},
			responseStatusCode:  http.StatusOK,
			wantStatus:          StatusReady,
			wantRequeueCountMin: 1,
			wantRequestCountMin: 1,
		},
		{
			name: "single pod - 404",
			pods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "ns",
						Name:      "p1",
						Labels:    map[string]string{"app": "p"},
					},
					Status: corev1.PodStatus{PodIP: "127.0.0.1"},
				},
			},
			podsLabelsSelector: labels.SelectorFromSet(map[string]string{"app": "p"}),
			addressable: Addressable{
				Address:     &url.URL{Scheme: "http", Path: "/b1/b1"},
				ResourceKey: types.NamespacedName{Namespace: "b1", Name: "b1"},
			},
			responseStatusCode:  http.StatusNotFound,
			wantStatus:          StatusNotReady,
			wantRequeueCountMin: 1,
			wantRequestCountMin: 1,
		},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := reconcilertesting.SetupFakeContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer func() {
				time.Sleep(time.Second)
				cancel()
			}()
			wantRequestCountMin := atomic.NewInt64(int64(tc.wantRequestCountMin))
			h := http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
				wantRequestCountMin.Dec()
				require.Equal(t, network.ProbeHeaderValue, r.Header.Get(network.ProbeHeaderName))
				writer.WriteHeader(tc.responseStatusCode)
			})
			s := httptest.NewUnstartedServer(h)
			s.Start()
			defer s.Close()

			for _, p := range tc.pods {
				_ = podinformer.Get(ctx).Informer().GetStore().Add(p)
			}
			tc.addressable.Address.Host = s.URL
			u, _ := url.Parse(s.URL)

			wantRequeueCountMin := atomic.NewInt64(int64(tc.wantRequeueCountMin))
			var IPsLister IPsLister = func() ([]string, error) {
				pods, err := podinformer.Get(ctx).Lister().List(tc.podsLabelsSelector)
				if err != nil {
					return nil, err
				}
				ips := make([]string, 0, len(pods))
				for _, p := range pods {
					ips = append(ips, p.Status.PodIP)
				}
				return ips, nil
			}
			prober := NewAsync(ctx, s.Client(), u.Port(), IPsLister, func(key types.NamespacedName) {
				wantRequeueCountMin.Dec()
			})

			probeFunc := func() bool {
				status := prober.Probe(ctx, tc.addressable, tc.wantStatus)
				return status == tc.wantStatus
			}

			require.Eventuallyf(t, probeFunc, 5*time.Second, 100*time.Millisecond, "")
			require.Eventuallyf(t, func() bool { return wantRequestCountMin.Load() == 0 }, 5*time.Second, 100*time.Millisecond, "got %d, want 0", wantRequestCountMin.Load())
			require.Eventuallyf(t, func() bool { return wantRequeueCountMin.Load() == 0 }, 5*time.Second, 100*time.Millisecond, "got %d, want 0", wantRequeueCountMin.Load())
		})
	}
}
