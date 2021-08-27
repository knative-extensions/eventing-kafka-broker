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

	"knative.dev/eventing/pkg/apis/messaging"

	"knative.dev/eventing-kafka/pkg/common/constants"
)

func (kc *KafkaChannel) SetDefaults(ctx context.Context) {
	// Set the duck subscription to the stored version of the duck
	// we support. Reason for this is that the stored version will
	// not get a chance to get modified, but for newer versions
	// conversion webhook will be able to take a crack at it and
	// can modify it to match the duck shape.
	if kc.Annotations == nil {
		kc.Annotations = make(map[string]string)
	}

	if _, ok := kc.Annotations[messaging.SubscribableDuckVersionAnnotation]; !ok {
		kc.Annotations[messaging.SubscribableDuckVersionAnnotation] = "v1"
	}

	kc.Spec.SetDefaults(ctx)
}

func (kcs *KafkaChannelSpec) SetDefaults(ctx context.Context) {
	if kcs.NumPartitions == 0 {
		kcs.NumPartitions = constants.DefaultNumPartitions
	}
	if kcs.ReplicationFactor == 0 {
		kcs.ReplicationFactor = constants.DefaultReplicationFactor
	}
	if len(kcs.RetentionDuration) <= 0 {
		kcs.RetentionDuration = constants.DefaultRetentionISO8601Duration
	}
}
