/*
 * Copyright 2021 The Knative Authors
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
	"testing"

	"github.com/google/go-cmp/cmp"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
)

func TestConsumerSetDefaults(t *testing.T) {
	tests := []struct {
		name  string
		ctx   context.Context
		given *Consumer
		want  *Consumer
	}{
		{
			name: "with delivery",
			ctx:  context.Background(),
			given: &Consumer{
				Spec: ConsumerSpec{Delivery: &DeliverySpec{}},
			},
			want: &Consumer{
				Spec: ConsumerSpec{Delivery: &DeliverySpec{InitialOffset: sources.OffsetLatest}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.given.SetDefaults(tt.ctx)
			if diff := cmp.Diff(tt.want, tt.given); diff != "" {
				t.Error("(-want, +got)", diff)
			}
		})
	}
}
