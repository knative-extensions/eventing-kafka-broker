/*
 * Copyright 2020 The Knative Authors
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

package sink

import (
	"testing"

	"github.com/stretchr/testify/assert"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	_ "knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake"

	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/eventing/v1alpha1/kafkasink/fake"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
)

func TestNewController(t *testing.T) {

	ctx, _ := reconcilertesting.SetupFakeContext(t)

	controller := NewController(ctx, nil, &config.Env{
		IngressPodPort: "8080",
	})

	assert.NotNil(t, controller, "controller is nil")
}

func loadCert() []byte {
	return []byte(`
-----BEGIN CERTIFICATE-----
MIIDmjCCAoKgAwIBAgIUYzA4bTMXevuk3pl2Mn8hpCYL2C0wDQYJKoZIhvcNAQEL
BQAwLzELMAkGA1UEBhMCVVMxIDAeBgNVBAMMF0tuYXRpdmUtRXhhbXBsZS1Sb290
LUNBMB4XDTIzMDQwNTEzMTUyNFoXDTI2MDEyMzEzMTUyNFowbTELMAkGA1UEBhMC
VVMxEjAQBgNVBAgMCVlvdXJTdGF0ZTERMA8GA1UEBwwIWW91ckNpdHkxHTAbBgNV
BAoMFEV4YW1wbGUtQ2VydGlmaWNhdGVzMRgwFgYDVQQDDA9sb2NhbGhvc3QubG9j
YWwwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC5teo+En6U5nhqn7Sc
uanqswUmPlgs9j/8l21Rhb4T+ezlYKGQGhbJyFFMuiCE1Rjn8bpCwi7Nnv12Y2nz
FhEv2Jx0yL3Tqx0Q593myqKDq7326EtbO7wmDT0XD03twH5i9XZ0L0ihPWn1mjUy
WxhnHhoFpXrsnQECJorZY6aTrFbGVYelIaj5AriwiqyL0fET8pueI2GwLjgWHFSH
X8XsGAlcLUhkQG0Z+VO9usy4M1Wpt+cL6cnTiQ+sRmZ6uvaj8fKOT1Slk/oUeAi4
WqFkChGzGzLik0QrhKGTdw3uUvI1F2sdQj0GYzXaWqRz+tP9qnXdzk1GrszKKSlm
WBTLAgMBAAGjcDBuMB8GA1UdIwQYMBaAFJJcCftus4vj98N0zQQautsjEu82MAkG
A1UdEwQCMAAwCwYDVR0PBAQDAgTwMBQGA1UdEQQNMAuCCWxvY2FsaG9zdDAdBgNV
HQ4EFgQUnu/3vqA3VEzm128x/hLyZzR9JlgwDQYJKoZIhvcNAQELBQADggEBAFc+
1cKt/CNjHXUsirgEhry2Mm96R6Yxuq//mP2+SEjdab+FaXPZkjHx118u3PPX5uTh
gTT7rMfka6J5xzzQNqJbRMgNpdEFH1bbc11aYuhi0khOAe0cpQDtktyuDJQMMv3/
3wu6rLr6fmENo0gdcyUY9EiYrglWGtdXhlo4ySRY8UZkUScG2upvyOhHTxVCAjhP
efbMkNjmDuZOMK+wqanqr5YV6zMPzkQK7DspfRgasMAQmugQu7r2MZpXg8Ilhro1
s/wImGnMVk5RzpBVrq2VB9SkX/ThTVYEC/Sd9BQM364MCR+TA1l8/ptaLFLuwyw8
O2dgzikq8iSy1BlRsVw=
-----END CERTIFICATE-----
`)
}