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
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	"knative.dev/pkg/network"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
)

var (
	CA  []byte
	Key []byte
	Crt []byte
)

func init() {
	CA, Key, Crt = loadCerts()
}

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
		useTLS              bool
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
			useTLS:              false,
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
			useTLS:              false,
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
			useTLS:              false,
		},
		{
			name: "single pod - TLS",
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
				Address:     &url.URL{Scheme: "https", Path: "/b1/b1"},
				ResourceKey: types.NamespacedName{Namespace: "b1", Name: "b1"},
			},
			responseStatusCode:  http.StatusOK,
			wantStatus:          StatusReady,
			wantRequeueCountMin: 1,
			wantRequestCountMin: 1,
			useTLS:              true,
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
			if tc.useTLS {
				cert, err := tls.X509KeyPair(Crt, Key)
				require.NoError(t, err)
				s.TLS = &tls.Config{
					Certificates: []tls.Certificate{cert},
				}
				s.StartTLS()
			} else {
				s.Start()
			}
			defer s.Close()

			for _, p := range tc.pods {
				_ = podinformer.Get(ctx).Informer().GetStore().Add(p)
			}
			tc.addressable.Address.Host = s.URL
			u, _ := url.Parse(s.URL)

			wantRequeueCountMin := atomic.NewInt64(int64(tc.wantRequeueCountMin))
			var IPsLister IPsLister = func(addressable Addressable) ([]string, error) {
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
			var prober Prober
			var err error
			if tc.useTLS {
				prober, err = NewAsyncWithTLS(ctx, u.Port(), IPsLister, func(key types.NamespacedName) {
					wantRequeueCountMin.Dec()
				}, pointer.String(string(CA)))
				require.NoError(t, err)
			} else {
				prober = NewAsync(ctx, s.Client(), u.Port(), IPsLister, func(key types.NamespacedName) {
					wantRequeueCountMin.Dec()
				})
			}

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

// Adapted from https://github.com/knative/eventing/blob/57d78e060db6e0d2f3046eeedd27137cfa4fe0bc/pkg/eventingtls/eventingtlstesting/eventingtlstesting.go#L84
func loadCerts() ([]byte, []byte, []byte) {
	/*
		Provisioned using:
		openssl req -x509 -nodes -new -sha256 -days 1024 -newkey rsa:2048 -keyout RootCA.key -out RootCA.pem -subj "/C=US/CN=Knative-Example-Root-CA"
		openssl x509 -outform pem -in RootCA.pem -out RootCA.crt
		openssl req -new -nodes -newkey rsa:2048 -keyout localhost.key -out localhost.csr -subj "/C=US/ST=YourState/L=YourCity/O=Example-Certificates/CN=localhost.local"
		openssl x509 -req -sha256 -days 1024 -in localhost.csr -CA RootCA.pem -CAkey RootCA.key -CAcreateserial -extfile domains.ext -out localhost.crt

		Copy:
		- RootCA.crt for ca
		- localhost.key for key
		- localhost.crt for crt

		domains.ext file:
		authorityKeyIdentifier=keyid,issuer
		basicConstraints=CA:FALSE
		keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
		subjectAltName = @alt_names
		[alt_names]
		DNS.1 = localhost
		IP.1 = 127.0.0.1
	*/
	return []byte(`
-----BEGIN CERTIFICATE-----
MIIDPzCCAiegAwIBAgIUOF3U5UMwffSmdo24IVU1k+qix3YwDQYJKoZIhvcNAQEL
BQAwLzELMAkGA1UEBhMCVVMxIDAeBgNVBAMMF0tuYXRpdmUtRXhhbXBsZS1Sb290
LUNBMB4XDTIzMDYwNjE0MDY1NFoXDTI2MDMyNjE0MDY1NFowLzELMAkGA1UEBhMC
VVMxIDAeBgNVBAMMF0tuYXRpdmUtRXhhbXBsZS1Sb290LUNBMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsJEA/+FW8e/ChmpseeH+UMtpP3PIq4VO26yh
fg3RSWKRbEnpkusWX6tM5NIZ9HqZOhB9dvb0OAC+YBM5ce8eA1/5tIUcxOzvMo5S
Oe+5cOgzZPLNesPBD+vteFXeD/9Hg75KfxctgyYfKqAE4Q8afaxs29/9K4wZkdE7
Fs4ED8r6hxf+7wgVSurnHiQnupHOb3BCQEGFm4w5/YJMhJFM29+LtIa5iZvQdlIC
zrIiLSckaRCiuJH2U5HCxk6WpodyoD5ffqzX7/+xismUwsX9opnMfdz7vT4ZYvKc
5O0u6/mx9fvhCL7hVwz8/FKvd1+Z4WnGoL/Iz3g+T/qdMbA+1wIDAQABo1MwUTAd
BgNVHQ4EFgQU51Q84l/eECxUhLRPhlcoLougg0owHwYDVR0jBBgwFoAU51Q84l/e
ECxUhLRPhlcoLougg0owDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOC
AQEAZVXtix62c6VVAEZHsSTPwlMwGjZ67UCd6NxeY5IgXdT/vmorlrsoZa0FYYkU
TdWOHt7Q1C48W+tA2yMTPGs240Zradam2CXAxEvL7/aC6GEFs7vhkq6riwJ/erR3
ZAZjcWi5Qk03q7eS61JJvaV9+fKg+F2BB2EqaCPo7HMMSXO81aeHEMl/AQsNPnur
2VG1tchMQvfakRf53H1hWu5h4APuZo1MTkPmBOTLZG7eAJTtfVWz1aPwB1rUMCyP
wSdZWoEx7ye2kUHEyRKdRGbHyJtY9YYvaROznzxqVpIqHxnRQnE/If7kcN4t/7vi
28zWIDKzJ8je40SPcLSfplRvBQ==
-----END CERTIFICATE-----`), []byte(`
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCodsq2l7lP1LMw
hh9j4FQvULIfvcwiz3WyBkeLvQPk044ZRX50bwm2llTKwEvWyqnmctjcS5RiKItw
9kg6eXA3Z9CBuPyJkrvH/4OO2wOBgyBD8k3LqNtaqN0Qq2Y3JbQ/PBf5btNMmYFB
gostqCBsiDIF1KqkpCHpvLBLPte9bLv4ZAxNecC6p/fXUbXTWJ4SFNhdZrw62Rs4
boq8qTs0PceDJiLdvqwngGNneCdehFXR3citMCA8SxyD8E2qIIOLuOEf54s9zR7C
d+D0h+XQmHICAcz3Yo4a27dNFUc2LHcp9a2+ASHTnKsR6Xhndo/IcT7/qbYq7/kJ
lfN66ObzAgMBAAECggEADBYpyRvtobqi+JJG4kWQBK0HepuFb+Hukc09iNsQ0nQT
N+Dyh6wHyF/UyY8uYcS8l9oZkQSjKr+58WraF8fqsy7xmL0K8VvjuR+t8qvn/nzH
7dgOmNQOmNyQr8d8V+yOmBLZrX20D0TcLzUMg0QSv3auEBkH/TQBcuGkzGE/3Uk4
DT/L//DREjVw+DAaFd53UWWhSnLOkQAsY+zR+HktQH0CEtwMjbkBWMgCkgX7/v9w
gBQLwR7uw5w0Kn8ArgqXj5b5naqHhNzMPj65kMHFjYejSDsjPntToz5YrDRsd4L3
EYAcKcG4fYjJ8vYbRjG7SYCxX7HsvJUhqZT4ds/DsQKBgQDTrSN8m60ZPwmvAG+6
Tpy3Hpf+klO/AE/FQ6WQ8K77McBoRA4awFcjqasqxkTHCl8FXXdWoEe+OIJ+MXzu
5zX5J1dAOl++sgdOvQFW4y4H+FOHD5q5SHFzdI8d65HJy6SI6BVJz4lR51bSU5CQ
qkdh0Sbh1hAACIsmZTApaXY03wKBgQDLvUShkRaJC/pzN7yCb0Cu4VxBV90IkE0n
INHNML8/KCbGJ5EmLk5uJt1sWb0e8PpUgoavnljfUUKyNfd/ltr9slE6uRhy9net
qg1A0CmArFJgOncA3bu92CvvzzcDPsnHCBnLKpTfUSThk8Sxftg0bqKEV3sy5Py1
9x6Sp7QcbQKBgQCKRjDHRn6F3mr5+aQCpTW0XXTWpEm2nIJ/jxgJnWAA0VgqBELe
cMS7lCsvLwNgrkKyI4NAgEU9WnbL7pH5EeptDqjtWPSQgoVJhyfn1VGNfUc7FBNz
c4JA9GRFHExI8RFTKaA2bi765M8PZ+0ow0ML/++RWR9slign9bPHaZABKwKBgHz9
unMcXaTqMlYpJX8n3ZjsLPrxemrcjFiq68tkUo/ehBsg/w1bb0ZolYL5curej9T0
1sg67u7iHXbTYOlnlSX7FZZfI76zsixanRLcIfoMveTHOWbQoXMQgbP3fhqBlKyE
Lb7UesyeLXAuhYcW+HECRrXGLZDFprvDxX/XXsnpAoGBAJdaCxiy7ZXDrJHJDzGp
Ntxv2SbGghJwlmWYh7BP/+Cb6vUWG4MTzUBIzKfk4Z32xjFwDxKi3SW+34uZ6/fD
Ptt315Oq0odZvrdGtJoGud/p9nCHUiLGHwRH9NrDtTcO9zR55oYc0pJk0EfrXpsb
r5IiDpxJPL1q0JmKeA+Fr4wy
-----END PRIVATE KEY-----`), []byte(`
-----BEGIN CERTIFICATE-----
MIIDoDCCAoigAwIBAgIUSVuHbk6clsj/7Fe3Uc8mFwXU6kMwDQYJKoZIhvcNAQEL
BQAwLzELMAkGA1UEBhMCVVMxIDAeBgNVBAMMF0tuYXRpdmUtRXhhbXBsZS1Sb290
LUNBMB4XDTIzMDYwNjE0MTEwNFoXDTI2MDMyNjE0MTEwNFowbTELMAkGA1UEBhMC
VVMxEjAQBgNVBAgMCVlvdXJTdGF0ZTERMA8GA1UEBwwIWW91ckNpdHkxHTAbBgNV
BAoMFEV4YW1wbGUtQ2VydGlmaWNhdGVzMRgwFgYDVQQDDA9sb2NhbGhvc3QubG9j
YWwwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCodsq2l7lP1LMwhh9j
4FQvULIfvcwiz3WyBkeLvQPk044ZRX50bwm2llTKwEvWyqnmctjcS5RiKItw9kg6
eXA3Z9CBuPyJkrvH/4OO2wOBgyBD8k3LqNtaqN0Qq2Y3JbQ/PBf5btNMmYFBgost
qCBsiDIF1KqkpCHpvLBLPte9bLv4ZAxNecC6p/fXUbXTWJ4SFNhdZrw62Rs4boq8
qTs0PceDJiLdvqwngGNneCdehFXR3citMCA8SxyD8E2qIIOLuOEf54s9zR7Cd+D0
h+XQmHICAcz3Yo4a27dNFUc2LHcp9a2+ASHTnKsR6Xhndo/IcT7/qbYq7/kJlfN6
6ObzAgMBAAGjdjB0MB8GA1UdIwQYMBaAFOdUPOJf3hAsVIS0T4ZXKC6LoINKMAkG
A1UdEwQCMAAwCwYDVR0PBAQDAgTwMBoGA1UdEQQTMBGCCWxvY2FsaG9zdIcEfwAA
ATAdBgNVHQ4EFgQUtxq3RVNeuFDQEu/I1Hn4u+aCKogwDQYJKoZIhvcNAQELBQAD
ggEBAIP9672LFvNaBWCvZybv62eUoALJzxGFtXTNa9YjkYHZLwJXBa/8cnCLfSiP
6uxUK3lDL4jF8I0VEWe2q3H2R8AllofFQbqeskD5qrrVjMdV/0tuUHI8RPCr9SPP
Y6wIq3dlk98ZlQEwhBz3M4SYpLKyKAn/E/2ScsW+9vcvAAAK32BO27Tk9Ca6ShtQ
p32q5PZOx9+eicXzW7qb4a26k1aFnnaDEUuSQsKXhzVVyt9Xmg14m8ETeEL5xPfI
PiUZitNmqpg2123YyPwE4NW8okkLO03UD3I0I/Bn0mS0sb8xMt/ncR4iWeJOvZSG
0YhYDYdUoSliRZYy5zTe7orFj7Q=
-----END CERTIFICATE-----`)
}
