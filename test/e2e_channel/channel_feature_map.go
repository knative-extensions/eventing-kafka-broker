//go:build e2e
// +build e2e

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

package e2echannel

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testlib "knative.dev/eventing/test/lib"

	messagingv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/messaging/v1beta1"
)

// Kind for messaging resources.
const (
	KafkaChannelKind string = "KafkaChannel"
)

// ChannelFeatureMap saves the channel-features mapping.
// Each pair means the channel support the list of features.
var ChannelFeatureMap = map[metav1.TypeMeta][]testlib.Feature{
	{
		APIVersion: messagingv1beta1.SchemeGroupVersion.String(),
		Kind:       KafkaChannelKind,
	}: {
		testlib.FeatureBasic,
		testlib.FeatureRedelivery,
		testlib.FeaturePersistence,
	},
}
