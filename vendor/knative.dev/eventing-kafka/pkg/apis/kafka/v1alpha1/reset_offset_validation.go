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

package v1alpha1

import (
	"context"
	"time"

	"knative.dev/pkg/apis"
	"knative.dev/pkg/kmp"
)

// Validate verifies the ResetOffset and returns errors for any invalid fields.
func (ro *ResetOffset) Validate(ctx context.Context) *apis.FieldError {
	errs := ro.Spec.Validate(ctx).ViaField("spec")

	if apis.IsInUpdate(ctx) {
		original := apis.GetBaseline(ctx).(*ResetOffset)
		errs = errs.Also(ro.CheckImmutableFields(ctx, original))
	}

	return errs
}

// Validate verifies the ResetOffsetSpec and returns errors for an invalid fields.
func (ros *ResetOffsetSpec) Validate(ctx context.Context) *apis.FieldError {

	var errs *apis.FieldError

	// Validate The Offset String ("earliest", "latest", or valid date string)
	if !ros.IsOffsetEarliest() && !ros.IsOffsetLatest() {
		offsetTime, err := ros.ParseOffsetTime()
		if err != nil || offsetTime.After(time.Now()) {
			errs = errs.Also(apis.ErrInvalidValue(ros.Offset.Time, "offset"))
		}
	}

	// Validate The Ref KReference Basics (Kafka Topic relation which is expected to be done in Controllers!)
	errs = errs.Also(ros.Ref.Validate(ctx))

	return errs
}

// CheckImmutableFields verifies the immutable spec fields have not been changed from the original.
func (ro *ResetOffset) CheckImmutableFields(_ context.Context, original *ResetOffset) *apis.FieldError {
	if original == nil {
		return nil
	}

	if diff, err := kmp.ShortDiff(original.Spec, ro.Spec); err != nil {
		return &apis.FieldError{
			Message: "Failed to diff ResetOffset",
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
