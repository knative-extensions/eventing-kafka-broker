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
	"fmt"

	"knative.dev/pkg/apis"

	bindingsv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	v1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"
)

// ConvertTo implements apis.Convertible
func (source *KafkaSource) ConvertTo(ctx context.Context, to apis.Convertible) error {
	switch sink := to.(type) {
	case *v1.KafkaSource:
		source.ObjectMeta.DeepCopyInto(&sink.ObjectMeta)
		sink.Spec = v1.KafkaSourceSpec{
			Consumers:     source.Spec.Consumers,
			KafkaAuthSpec: *source.Spec.KafkaAuthSpec.ConvertToV1(ctx),
			Topics:        source.Spec.Topics,
			ConsumerGroup: source.Spec.ConsumerGroup,
			InitialOffset: v1.Offset(source.Spec.InitialOffset),
			Delivery:      source.Spec.Delivery,
			Ordering:      (*v1.DeliveryOrdering)(source.Spec.Ordering),
			SourceSpec:    source.Spec.SourceSpec,
		}
		sink.Status = v1.KafkaSourceStatus{
			SourceStatus: *source.Status.SourceStatus.DeepCopy(),
			Consumers:    source.Status.Consumers,
			Selector:     source.Status.Selector,
			Claims:       source.Status.Claims,
			Placeable:    source.Status.Placeable,
		}
		return nil
	default:
		return fmt.Errorf("unknown version, got: %T", sink)
	}
}

// ConvertFrom implements apis.Convertible
func (sink *KafkaSource) ConvertFrom(ctx context.Context, from apis.Convertible) error {

	switch source := from.(type) {
	case *v1.KafkaSource:
		source.ObjectMeta.DeepCopyInto(&sink.ObjectMeta)
		authSpec := bindingsv1beta1.KafkaAuthSpec{}
		authSpec.ConvertFromV1(&source.Spec.KafkaAuthSpec)
		sink.Spec = KafkaSourceSpec{
			Consumers:     source.Spec.Consumers,
			KafkaAuthSpec: authSpec,
			Topics:        source.Spec.Topics,
			ConsumerGroup: source.Spec.ConsumerGroup,
			InitialOffset: Offset(source.Spec.InitialOffset),
			Delivery:      source.Spec.Delivery,
			Ordering:      (*DeliveryOrdering)(source.Spec.Ordering),
			SourceSpec:    source.Spec.SourceSpec,
		}
		sink.Status = KafkaSourceStatus{
			SourceStatus: source.Status.SourceStatus,
			Consumers:    source.Status.Consumers,
			Selector:     source.Status.Selector,
			Claims:       source.Status.Claims,
			Placeable:    source.Status.Placeable,
		}

		return nil
	default:
		return fmt.Errorf("unknown version, got: %T", source)
	}
}
