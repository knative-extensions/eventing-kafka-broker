/*
Copyright 2020 The Knative Authors

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

package v1beta1

import (
	"context"
	"strconv"

	"github.com/google/uuid"
	"k8s.io/utils/ptr"

	"knative.dev/pkg/apis"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/config"
)

const (
	uuidPrefix = "knative-kafka-source-"

	classAnnotation             = "autoscaling.knative.dev/class"
	minScaleAnnotation          = "autoscaling.knative.dev/minScale"
	maxScaleAnnotation          = "autoscaling.knative.dev/maxScale"
	pollingIntervalAnnotation   = "keda.autoscaling.knative.dev/pollingInterval"
	cooldownPeriodAnnotation    = "keda.autoscaling.knative.dev/cooldownPeriod"
	kafkaLagThresholdAnnotation = "keda.autoscaling.knative.dev/kafkaLagThreshold"
)

// SetDefaults ensures KafkaSource reflects the default values.
func (ks *KafkaSource) SetDefaults(ctx context.Context) {
	ctx = apis.WithinParent(ctx, ks.ObjectMeta)

	if ks.Spec.ConsumerGroup == "" {
		ks.Spec.ConsumerGroup = uuidPrefix + uuid.New().String()
	}

	if ks.Spec.Consumers == nil {
		ks.Spec.Consumers = ptr.To(int32(1))
	}

	if ks.Spec.InitialOffset == "" {
		ks.Spec.InitialOffset = OffsetLatest
	}

	if ks.Spec.Ordering == nil {
		deliveryOrdering := Ordered
		ks.Spec.Ordering = &deliveryOrdering
	}

	kafkaConfig := config.FromContextOrDefaults(ctx)
	kafkaDefaults := kafkaConfig.KafkaSourceDefaults
	if kafkaDefaults.AutoscalingClass == config.KedaAutoscalingClass {
		if ks.Annotations == nil {
			ks.Annotations = map[string]string{}
		}
		ks.Annotations[classAnnotation] = kafkaDefaults.AutoscalingClass

		// Set all annotations regardless of defaults
		ks.Annotations[minScaleAnnotation] = strconv.FormatInt(kafkaDefaults.MinScale, 10)
		ks.Annotations[maxScaleAnnotation] = strconv.FormatInt(kafkaDefaults.MaxScale, 10)
		ks.Annotations[pollingIntervalAnnotation] = strconv.FormatInt(kafkaDefaults.PollingInterval, 10)
		ks.Annotations[cooldownPeriodAnnotation] = strconv.FormatInt(kafkaDefaults.CooldownPeriod, 10)
		ks.Annotations[kafkaLagThresholdAnnotation] = strconv.FormatInt(kafkaDefaults.KafkaLagThreshold, 10)
	}

	ks.Spec.Sink.SetDefaults(ctx)
	ks.Spec.Delivery.SetDefaults(ctx)
}
