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
	"crypto/tls"
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
	"k8s.io/utils/pointer"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	"knative.dev/pkg/network"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
)

func TestCompositeProber(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name                     string
		pods                     []*corev1.Pod
		podsLabelsSelector       labels.Selector
		addressable              ProberAddressable
		responseStatusCode       int
		wantStatus               Status
		wantRequeueCountMin      int
		wantHttpRequestCountMin  int
		wantHttpsRequestCountMin int
	}{
		{
			name: "one pod - http only",
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
			addressable: ProberAddressable{
				AddressStatus: &duckv1.AddressStatus{
					Address: &duckv1.Addressable{
						URL: &apis.URL{Scheme: "http", Path: "/b1/b1"},
					},
					Addresses: []duckv1.Addressable{
						{
							URL: &apis.URL{Scheme: "http", Path: "/b1/b1"},
						},
					},
				},
			},
			responseStatusCode:       http.StatusOK,
			wantStatus:               StatusReady,
			wantRequeueCountMin:      1,
			wantHttpRequestCountMin:  1,
			wantHttpsRequestCountMin: 0,
		},
		{
			name: "one pod - https only",
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
			addressable: ProberAddressable{
				AddressStatus: &duckv1.AddressStatus{
					Address: &duckv1.Addressable{
						URL: &apis.URL{Scheme: "https", Path: "/b1/b1"},
					},
					Addresses: []duckv1.Addressable{
						{
							URL: &apis.URL{Scheme: "https", Path: "/b1/b1"},
						},
					},
				},
			},
			responseStatusCode:       http.StatusOK,
			wantStatus:               StatusReady,
			wantRequeueCountMin:      1,
			wantHttpRequestCountMin:  0,
			wantHttpsRequestCountMin: 1,
		},
		{
			name: "one pod - http and https, http primary address",
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
			addressable: ProberAddressable{
				AddressStatus: &duckv1.AddressStatus{
					Address: &duckv1.Addressable{
						URL: &apis.URL{Scheme: "http", Path: "/b1/b1"},
					},
					Addresses: []duckv1.Addressable{
						{
							URL: &apis.URL{Scheme: "http", Path: "/b1/b1"},
						},
						{
							URL: &apis.URL{Scheme: "https", Path: "/b1/b1"},
						},
					},
				},
			},
			responseStatusCode:       http.StatusOK,
			wantStatus:               StatusReady,
			wantRequeueCountMin:      2,
			wantHttpRequestCountMin:  1,
			wantHttpsRequestCountMin: 1,
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
			wantHttpRequestCountMin := atomic.NewInt64(int64(tc.wantHttpRequestCountMin))
			httpHandler := http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
				wantHttpRequestCountMin.Dec()
				require.Equal(t, network.ProbeHeaderValue, r.Header.Get(network.ProbeHeaderName))
				writer.WriteHeader(tc.responseStatusCode)
			})
			httpServer := httptest.NewUnstartedServer(httpHandler)
			httpServer.Start()
			defer httpServer.Close()

			wantHttpsRequestCountMin := atomic.NewInt64(int64(tc.wantHttpsRequestCountMin))
			httpsHandler := http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
				wantHttpsRequestCountMin.Dec()
				require.Equal(t, network.ProbeHeaderValue, r.Header.Get(network.ProbeHeaderName))
				writer.WriteHeader(tc.responseStatusCode)
			})
			httpsServer := httptest.NewUnstartedServer(httpsHandler)
			cert, err := tls.X509KeyPair(Crt1, Key1)
			require.NoError(t, err)
			httpsServer.TLS = &tls.Config{
				Certificates: []tls.Certificate{cert},
			}
			httpsServer.StartTLS()
			defer httpsServer.Close()

			for _, p := range tc.pods {
				_ = podinformer.Get(ctx).Informer().GetStore().Add(p)
			}

			for _, addr := range tc.addressable.AddressStatus.Addresses {
				if addr.URL.Scheme == "http" {
					addr.URL.Host = httpServer.URL
				} else {
					addr.URL.Host = httpsServer.URL
				}
			}

			var IPsLister IPsLister = func(addressable proberAddressable) ([]string, error) {
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

			httpUrl, _ := url.Parse(httpServer.URL)
			httpsUrl, _ := url.Parse(httpsServer.URL)
			requeueCountMin := atomic.NewInt64(int64(tc.wantRequeueCountMin))
			prober, err := NewComposite(ctx, httpUrl.Port(), httpsUrl.Port(), IPsLister, func(key types.NamespacedName) {
				requeueCountMin.Dec()
			}, pointer.String(string(CA1)))
			require.NoError(t, err)

			probeFunc := func() bool {
				status := prober.Probe(ctx, tc.addressable, tc.wantStatus)
				return status == tc.wantStatus
			}

			require.Eventuallyf(t, probeFunc, 5*time.Second, 250*time.Millisecond, "")
			require.Eventuallyf(t, func() bool { return wantHttpRequestCountMin.Load() == 0 }, 5*time.Second, 250*time.Millisecond, "got %d, want 0", wantHttpRequestCountMin.Load())
			require.Eventuallyf(t, func() bool { return wantHttpsRequestCountMin.Load() == 0 }, 5*time.Second, 250*time.Millisecond, "got %d, want 0", wantHttpsRequestCountMin.Load())
			require.Eventuallyf(t, func() bool { return requeueCountMin.Load() == 0 }, 5*time.Second, 250*time.Millisecond, "got %d, want 0", requeueCountMin.Load())

		})
	}
}

func TestCompositeProberNoTLS(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name                     string
		pods                     []*corev1.Pod
		podsLabelsSelector       labels.Selector
		addressable              ProberAddressable
		responseStatusCode       int
		wantStatus               Status
		wantRequeueCountMin      int
		wantHttpRequestCountMin  int
		wantHttpsRequestCountMin int
	}{
		{
			name: "one pod - http only",
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
			addressable: ProberAddressable{
				AddressStatus: &duckv1.AddressStatus{
					Address: &duckv1.Addressable{
						URL: &apis.URL{Scheme: "http", Path: "/b1/b1"},
					},
					Addresses: []duckv1.Addressable{
						{
							URL: &apis.URL{Scheme: "http", Path: "/b1/b1"},
						},
					},
				},
			},
			responseStatusCode:       http.StatusOK,
			wantStatus:               StatusReady,
			wantRequeueCountMin:      1,
			wantHttpRequestCountMin:  1,
			wantHttpsRequestCountMin: 0,
		},
		{
			name: "one pod - https only",
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
			addressable: ProberAddressable{
				AddressStatus: &duckv1.AddressStatus{
					Address: &duckv1.Addressable{
						URL: &apis.URL{Scheme: "https", Path: "/b1/b1"},
					},
					Addresses: []duckv1.Addressable{
						{
							URL: &apis.URL{Scheme: "https", Path: "/b1/b1"},
						},
					},
				},
			},
			responseStatusCode:       http.StatusOK,
			wantStatus:               StatusUnknownErr,
			wantRequeueCountMin:      1,
			wantHttpRequestCountMin:  0,
			wantHttpsRequestCountMin: 0,
		},
		{
			name: "one pod - http and https, http primary address",
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
			addressable: ProberAddressable{
				AddressStatus: &duckv1.AddressStatus{
					Address: &duckv1.Addressable{
						URL: &apis.URL{Scheme: "http", Path: "/b1/b1"},
					},
					Addresses: []duckv1.Addressable{
						{
							URL: &apis.URL{Scheme: "http", Path: "/b1/b1"},
						},
						{
							URL: &apis.URL{Scheme: "https", Path: "/b1/b1"},
						},
					},
				},
			},
			responseStatusCode:       http.StatusOK,
			wantStatus:               StatusUnknownErr,
			wantRequeueCountMin:      2,
			wantHttpRequestCountMin:  1,
			wantHttpsRequestCountMin: 0,
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
			wantHttpRequestCountMin := atomic.NewInt64(int64(tc.wantHttpRequestCountMin))
			httpHandler := http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
				wantHttpRequestCountMin.Dec()
				require.Equal(t, network.ProbeHeaderValue, r.Header.Get(network.ProbeHeaderName))
				writer.WriteHeader(tc.responseStatusCode)
			})
			httpServer := httptest.NewUnstartedServer(httpHandler)
			httpServer.Start()
			defer httpServer.Close()

			wantHttpsRequestCountMin := atomic.NewInt64(int64(tc.wantHttpsRequestCountMin))
			httpsHandler := http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
				wantHttpsRequestCountMin.Dec()
				require.Equal(t, network.ProbeHeaderValue, r.Header.Get(network.ProbeHeaderName))
				writer.WriteHeader(tc.responseStatusCode)
			})
			httpsServer := httptest.NewUnstartedServer(httpsHandler)
			cert, err := tls.X509KeyPair(Crt1, Key1)
			require.NoError(t, err)
			httpsServer.TLS = &tls.Config{
				Certificates: []tls.Certificate{cert},
			}
			httpsServer.StartTLS()
			defer httpsServer.Close()

			for _, p := range tc.pods {
				_ = podinformer.Get(ctx).Informer().GetStore().Add(p)
			}

			for _, addr := range tc.addressable.AddressStatus.Addresses {
				if addr.URL.Scheme == "http" {
					addr.URL.Host = httpServer.URL
				} else {
					addr.URL.Host = httpsServer.URL
				}
			}

			var IPsLister IPsLister = func(addressable proberAddressable) ([]string, error) {
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

			httpUrl, _ := url.Parse(httpServer.URL)
			requeueCountMin := atomic.NewInt64(int64(0))
			prober, err := NewCompositeNoTLS(ctx, httpUrl.Port(), IPsLister, func(key types.NamespacedName) {
				requeueCountMin.Inc()
				t.Logf("Requeing prober, requeueCount: %d", requeueCountMin.Load())
			})
			require.NoError(t, err)

			probeFunc := func() bool {
				t.Logf("addressable: %+v", tc.addressable.AddressStatus.Addresses)
				status := prober.Probe(ctx, tc.addressable, StatusReady)
				return status == tc.wantStatus
			}

			require.Eventuallyf(t, probeFunc, 5*time.Second, 250*time.Millisecond, "")
			require.Eventuallyf(t, func() bool { return wantHttpRequestCountMin.Load() == 0 }, 5*time.Second, 250*time.Millisecond, "got %d, want 0", wantHttpRequestCountMin.Load())
			require.Eventuallyf(t, func() bool { return wantHttpsRequestCountMin.Load() == 0 }, 5*time.Second, 250*time.Millisecond, "got %d, want 0", wantHttpsRequestCountMin.Load())
			// As we are expecting some of these tests to never resolve to Ready, the prober will potentially requeue more than the min number of times during the test period
			// So, we want to test that we got at least as many requeues as the min we expect
			require.Eventuallyf(t, func() bool { return requeueCountMin.Load() >= int64(tc.wantRequeueCountMin) }, 5*time.Second, 250*time.Millisecond, "got %d, want 0", requeueCountMin.Load())

		})
	}

}
