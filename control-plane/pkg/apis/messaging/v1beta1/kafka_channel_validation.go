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

	"github.com/google/go-cmp/cmp"

	"github.com/google/go-cmp/cmp/cmpopts"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/kmp"
)

func (kc *KafkaChannel) Validate(ctx context.Context) *apis.FieldError {
	errs := kc.Spec.Validate(ctx).ViaField("spec")

	// Validate annotations
	if kc.Annotations != nil {
		if scope, ok := kc.Annotations[eventing.ScopeAnnotationKey]; ok {
			if scope != "namespace" && scope != "cluster" {
				iv := apis.ErrInvalidValue(scope, "")
				iv.Details = "expected either 'cluster' or 'namespace'"
				errs = errs.Also(iv.ViaFieldKey("annotations", eventing.ScopeAnnotationKey).ViaField("metadata"))
			}
		}
	}

	if apis.IsInUpdate(ctx) {
		original := apis.GetBaseline(ctx).(*KafkaChannel)
		errs = errs.Also(kc.CheckImmutableFields(ctx, original))
	}

	return errs
}

func (kcs *KafkaChannelSpec) Validate(ctx context.Context) *apis.FieldError {
	var errs *apis.FieldError

	if kcs.NumPartitions <= 0 {
		fe := apis.ErrInvalidValue(kcs.NumPartitions, "numPartitions")
		errs = errs.Also(fe)
	}

	if kcs.ReplicationFactor <= 0 {
		fe := apis.ErrInvalidValue(kcs.ReplicationFactor, "replicationFactor")
		errs = errs.Also(fe)
	}

	retentionDuration, err := kcs.ParseRetentionDuration()
	if retentionDuration < 0 || err != nil {
		fe := apis.ErrInvalidValue(kcs.RetentionDuration, "retentionDuration")
		errs = errs.Also(fe)
	}

	for i, subscriber := range kcs.SubscribableSpec.Subscribers {
		if subscriber.ReplyURI == nil && subscriber.SubscriberURI == nil {
			fe := apis.ErrMissingField("replyURI", "subscriberURI")
			fe.Details = "expected at least one of, got none"
			errs = errs.Also(fe.ViaField(fmt.Sprintf("subscriber[%d]", i)).ViaField("subscribable"))
		}
	}
	return errs
}

func (kc *KafkaChannel) CheckImmutableFields(_ context.Context, original *KafkaChannel) *apis.FieldError {
	if original == nil {
		return nil
	}

	ignoreArguments := []cmp.Option{cmpopts.IgnoreFields(KafkaChannelSpec{}, "ChannelableSpec")}

	// In the specific case of the original RetentionDuration being an empty string, allow it
	// as an exception to the immutability requirement.
	//
	// KafkaChannels created pre-v0.26 will not have a RetentionDuration field (thus an empty
	// string), and in v0.26 there is a post-install job that updates this to its proper value.
	// This immutability check was added after the post-install job, and without this exception
	// it will fail attempting to upgrade those pre-v0.26 channels.
	if original.Spec.RetentionDuration == "" && kc.Spec.RetentionDuration != "" {
		ignoreArguments = append(ignoreArguments, cmpopts.IgnoreFields(KafkaChannelSpec{}, "RetentionDuration"))
	}

	if diff, err := kmp.ShortDiff(original.Spec, kc.Spec, ignoreArguments...); err != nil {
		return &apis.FieldError{
			Message: "Failed to diff KafkaChannel",
			Paths:   []string{"spec"},
			Details: err.Error(),
		}
	} else if diff != "" {
		return &apis.FieldError{
			Message: "Immutable fields changed (-old +new)",
			Paths:   []string{"spec"},
			Details: diff,
		}
	}

	return nil
}
