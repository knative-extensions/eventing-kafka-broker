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

	"knative.dev/pkg/apis"
	"knative.dev/pkg/kmp"
)

// Validate ensures KafkaSource is properly configured.
func (ks *KafkaSource) Validate(ctx context.Context) *apis.FieldError {
	errs := ks.Spec.Validate(ctx).ViaField("spec")
	if apis.IsInUpdate(ctx) {
		original := apis.GetBaseline(ctx).(*KafkaSource)
		errs = errs.Also(ks.CheckImmutableFields(ctx, original))
	}
	return errs
}

func (kss *KafkaSourceSpec) Validate(ctx context.Context) *apis.FieldError {
	var errs *apis.FieldError

	// Validate source spec
	errs = errs.Also(kss.SourceSpec.Validate(ctx))

	// Check for mandatory fields
	if len(kss.Topics) <= 0 {
		errs = errs.Also(apis.ErrMissingField("topics"))
	}
	if len(kss.BootstrapServers) <= 0 {
		errs = errs.Also(apis.ErrMissingField("bootstrapServers"))
	}
	switch kss.InitialOffset {
	case OffsetEarliest, OffsetLatest:
	default:
		errs = errs.Also(apis.ErrInvalidValue(kss.InitialOffset, "initialOffset"))
	}

	return errs
}

func (ks *KafkaSource) CheckImmutableFields(ctx context.Context, original *KafkaSource) *apis.FieldError {
	if original == nil {
		return nil
	}
	diff, err := kmp.ShortDiff(original.Spec.ConsumerGroup, ks.Spec.ConsumerGroup)

	if err != nil {
		return &apis.FieldError{
			Message: "Failed to diff KafkaSource",
			Paths:   []string{"spec"},
			Details: err.Error(),
		}
	}

	if diff != "" {
		return &apis.FieldError{
			Message: "Immutable fields changed (-old +new)",
			Paths:   []string{"spec"},
			Details: diff,
		}
	}

	return nil
}
