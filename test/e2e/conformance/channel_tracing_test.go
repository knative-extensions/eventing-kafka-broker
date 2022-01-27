//go:build e2e
// +build e2e

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

package conformance

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/test/conformance/helpers"
	testlib "knative.dev/eventing/test/lib"

	contribtest "knative.dev/eventing-kafka/test"
)

// Eventing channels are v1, but Kafka channel is v1beta1.
const kafkaChannelAPIVersion = "messaging.knative.dev/v1beta1"

func TestChannelTracingWithReply(t *testing.T) {
	// Enable this test only for Kafka
	helpers.ChannelTracingTestHelperWithChannelTestRunner(context.Background(), t, testlib.ComponentsTestRunner{
		ComponentFeatureMap: map[metav1.TypeMeta][]testlib.Feature{
			{
				APIVersion: kafkaChannelAPIVersion,
				Kind:       contribtest.KafkaChannelKind,
			}: {
				testlib.FeatureBasic,
				testlib.FeatureRedelivery,
				testlib.FeaturePersistence,
			},
		},
		ComponentsToTest: []metav1.TypeMeta{
			{
				APIVersion: kafkaChannelAPIVersion,
				Kind:       contribtest.KafkaChannelKind,
			},
		},
	}, testlib.SetupClientOptionNoop)
}
