/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package upgrade

import (
	"context"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"knative.dev/eventing-kafka/test/e2e/helpers"

	eventinghelpers "knative.dev/eventing/test/e2e/helpers"
)

func runChannelSmokeTest(t *testing.T) {
	cases := smokeTestCases()
	ctx := context.Background()
	for i := range cases {
		tt := cases[i]
		t.Run(tt.name, func(t *testing.T) {
			eventinghelpers.SingleEventForChannelTestHelper(
				ctx, t, tt.encoding, tt.version,
				"", channelTestRunner,
			)
		})
	}
}

func runSourceSmokeTest(t *testing.T) {
	helpers.AssureKafkaSourceIsOperational(t, func(auth, testCase, version string) bool {
		return auth == "plain" && (testCase == "structured" || testCase == "binary")
	})
}

type smokeTestCase struct {
	name     string
	encoding cloudevents.Encoding
	version  eventinghelpers.SubscriptionVersion
}

func smokeTestCases() []smokeTestCase {
	return []smokeTestCase{{
		name:     "BinaryV1",
		encoding: cloudevents.EncodingBinary,
		version:  eventinghelpers.SubscriptionV1,
	}, {
		name:     "StructuredV1",
		encoding: cloudevents.EncodingStructured,
		version:  eventinghelpers.SubscriptionV1,
	}}
}
