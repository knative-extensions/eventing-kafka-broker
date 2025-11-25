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

	v1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/messaging/v1"
)

// ConvertTo implements apis.Convertible
func (kc *KafkaChannel) ConvertTo(_ context.Context, to apis.Convertible) error {
	switch sink := to.(type) {
	case *v1.KafkaChannel:
		kc.ObjectMeta.DeepCopyInto(&sink.ObjectMeta)
		sink.Spec = v1.KafkaChannelSpec{
			NumPartitions:     kc.Spec.NumPartitions,
			ReplicationFactor: kc.Spec.ReplicationFactor,
			RetentionDuration: kc.Spec.RetentionDuration,
			ChannelableSpec:   *kc.Spec.ChannelableSpec.DeepCopy(),
		}
		sink.Status = v1.KafkaChannelStatus{
			ChannelableStatus: *kc.Status.ChannelableStatus.DeepCopy(),
		}
		return nil
	default:
		return fmt.Errorf("unknown version, got: %T", sink)
	}
}

// ConvertFrom implements apis.Convertible
func (kc *KafkaChannel) ConvertFrom(_ context.Context, from apis.Convertible) error {
	switch channel := from.(type) {
	case *v1.KafkaChannel:
		channel.ObjectMeta.DeepCopyInto(&kc.ObjectMeta)
		kc.Spec = KafkaChannelSpec{
			NumPartitions:     channel.Spec.NumPartitions,
			ReplicationFactor: channel.Spec.ReplicationFactor,
			RetentionDuration: channel.Spec.RetentionDuration,
			ChannelableSpec:   *channel.Spec.ChannelableSpec.DeepCopy(),
		}
		kc.Status = KafkaChannelStatus{
			ChannelableStatus: *channel.Status.ChannelableStatus.DeepCopy(),
		}
		return nil
	default:
		return fmt.Errorf("unknown version, got: %T", channel)
	}
}
