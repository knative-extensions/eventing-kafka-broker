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

package kafka

import (
	pkgreconciler "knative.dev/pkg/reconciler"

	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
)

const (
	// Kafka broker class annotation value.
	BrokerClass     = "Kafka"
	BrokerClassYolo = "KafkaYolo"
)

func BrokerClassFilter() func(interface{}) bool {
	return pkgreconciler.AnnotationFilterFunc(
		brokerreconciler.ClassAnnotationKey,
		BrokerClass,
		false, // allowUnset
	)
}

func BrokerYoloClassFilter() func(interface{}) bool {
	return pkgreconciler.AnnotationFilterFunc(
		brokerreconciler.ClassAnnotationKey,
		BrokerClassYolo,
		false, // allowUnset
	)
}
