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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
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
	"knative.dev/eventing/pkg/eventingtls"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	"knative.dev/pkg/network"
	pointer "knative.dev/pkg/ptr"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
)

var (
	CA1  []byte
	Key1 []byte
	Crt1 []byte
	CA2  []byte
	Key2 []byte
	Crt2 []byte
)

func init() {
	CA1, Key1, Crt1, CA2, Key2, Crt2 = loadCerts()
}

func TestAsyncProber(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name                string
		pods                []*corev1.Pod
		podsLabelsSelector  labels.Selector
		addressable         proberAddressable
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
			addressable: proberAddressable{
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
			addressable: proberAddressable{
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
			addressable: proberAddressable{
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
			addressable: proberAddressable{
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
				cert, err := tls.X509KeyPair(Crt1, Key1)
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

			clientConfig := eventingtls.NewDefaultClientConfig()
			clientConfig.CACerts = pointer.String(string(CA1))

			tlsConfig, err := eventingtls.GetTLSClientConfig(clientConfig)
			if err != nil {
				t.Fatal(fmt.Errorf("failed to get TLS client config: %w", err))
			}
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = tlsConfig

			prober := NewAsync(ctx, &http.Client{Transport: transport}, u.Port(), IPsLister, func(key types.NamespacedName) {
				wantRequeueCountMin.Dec()
			})

			probeFunc := func() bool {
				status := prober.probe(ctx, tc.addressable, tc.wantStatus)
				return status == tc.wantStatus
			}

			require.Eventuallyf(t, probeFunc, 5*time.Second, 250*time.Millisecond, "")
			require.Eventuallyf(t, func() bool { return wantRequestCountMin.Load() == 0 }, 5*time.Second, 250*time.Millisecond, "got %d, want 0", wantRequestCountMin.Load())
			require.Eventuallyf(t, func() bool { return wantRequeueCountMin.Load() == 0 }, 5*time.Second, 250*time.Millisecond, "got %d, want 0", wantRequeueCountMin.Load())
		})
	}
}

// Adapted from https://github.com/knative/eventing/blob/57d78e060db6e0d2f3046eeedd27137cfa4fe0bc/pkg/eventingtls/eventingtlstesting/eventingtlstesting.go#L84
func loadCerts() ([]byte, []byte, []byte, []byte, []byte, []byte) {
	ca1Cert, _ /* ca1Key */, srv1Cert, srv1Key := mustChain("Knative-Example-Root-CA")
	ca2Cert, _ /* ca2Key */, srv2Cert, srv2Key := mustChain("Example-Root-CA")

	return ca1Cert, srv1Key, srv1Cert,
		ca2Cert, srv2Key, srv2Cert
}

func mustChain(cn string) ([]byte, []byte, []byte, []byte) {
	caCert, caKey := mustCA(cn)
	srvKey, csr := mustCSR()
	srvCert := mustSign(csr, caCert, caKey)

	return pemCert(caCert.Raw),
		pemKey(caKey),
		pemCert(srvCert),
		pemKey(srvKey)
}

// --- CA (openssl req -x509) ---
func mustCA(cn string) (*x509.Certificate, *rsa.PrivateKey) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			Country:    []string{"US"},
			CommonName: cn,
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(1024 * 24 * time.Hour),

		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign |
			x509.KeyUsageCRLSign |
			x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}

	cert, _ := x509.ParseCertificate(der)
	return cert, key
}

// --- CSR (openssl req -new) ---
func mustCSR() (*rsa.PrivateKey, []byte) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	csrTmpl := &x509.CertificateRequest{
		Subject: pkix.Name{
			Country:      []string{"US"},
			Province:     []string{"YourState"},
			Locality:     []string{"YourCity"},
			Organization: []string{"Example-Certificates"},
			CommonName:   "localhost.local",
		},
	}

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, csrTmpl, key)
	if err != nil {
		panic(err)
	}

	return key, csrDER
}

// --- Sign (openssl x509 -req -extfile domains.ext) ---
func mustSign(csrDER []byte, ca *x509.Certificate, caKey *rsa.PrivateKey) []byte {
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		panic(err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      csr.Subject,

		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(1024 * 24 * time.Hour),

		// domains.ext equivalent
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},

		BasicConstraintsValid: true,
		IsCA:                  false,

		KeyUsage: x509.KeyUsageDigitalSignature |
			x509.KeyUsageContentCommitment | // nonRepudiation
			x509.KeyUsageKeyEncipherment |
			x509.KeyUsageDataEncipherment,

		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, csr.PublicKey, caKey)
	if err != nil {
		panic(err)
	}

	return der
}

// --- PEM helpers ---
func pemCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})
}

func pemKey(key *rsa.PrivateKey) []byte {
	b, _ := x509.MarshalPKCS8PrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: b,
	})
}
