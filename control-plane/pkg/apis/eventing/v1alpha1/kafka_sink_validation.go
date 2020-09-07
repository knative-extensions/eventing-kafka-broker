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

package v1alpha1

import (
	"context"
	"fmt"

	"knative.dev/pkg/apis"
)

func (ks *KafkaSink) Validate(ctx context.Context) *apis.FieldError {
	var errs *apis.FieldError

	// validate spec
	errs = errs.Also(ks.Spec.Validate(ctx).ViaField("spec"))

	// check immutable fields
	if apis.IsInUpdate(ctx) {
		original := apis.GetBaseline(ctx).(*KafkaSink)
		errs = errs.Also(ks.CheckImmutableFields(ctx, original))
	}

	return errs
}

func (kss *KafkaSinkSpec) Validate(ctx context.Context) *apis.FieldError {
	var errs *apis.FieldError

	// check content mode value
	if !allowedContentModes.Has(*kss.ContentMode) {
		errs = errs.Also(apis.ErrInvalidValue(*kss.ContentMode, "contentMode"))
	}

	if kss.BootstrapServers == "" {
		errs = errs.Also(apis.ErrInvalidValue(kss.BootstrapServers, "bootstrapServers"))
	}

	if kss.Topic == "" {
		errs = errs.Also(apis.ErrInvalidValue(kss.Topic, "topic"))
	}

	return errs
}

func (ks *KafkaSink) CheckImmutableFields(ctx context.Context, original *KafkaSink) *apis.FieldError {

	var errs *apis.FieldError

	errs = errs.Also(ks.Spec.CheckImmutableFields(ctx, &original.Spec).ViaField("spec"))

	return errs
}

func (kss *KafkaSinkSpec) CheckImmutableFields(ctx context.Context, original *KafkaSinkSpec) *apis.FieldError {

	var errs *apis.FieldError

	if kss.Topic != original.Topic {
		errs = errs.Also(ErrImmutableField("topic"))
	}

	if kss.ReplicationFactor != original.ReplicationFactor {
		errs = errs.Also(ErrImmutableField("replicationFactor"))
	}

	if kss.NumPartitions != original.NumPartitions {
		errs = errs.Also(ErrImmutableField("numPartitions"))
	}

	return errs
}

func ErrImmutableField(field string) *apis.FieldError {
	return &apis.FieldError{
		Message: fmt.Sprintf("Immutable field updated"),
		Paths:   []string{field},
	}
}
