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
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConsumer_GetConsumerGroup(t *testing.T) {
	tests := []struct {
		name       string
		ObjectMeta metav1.ObjectMeta
		want       *metav1.OwnerReference
	}{
		{
			name:       "no owner reference",
			ObjectMeta: metav1.ObjectMeta{},
			want:       nil,
		},
		{
			name: "no CG owner reference",
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "Pod",
						Name:       "name",
					},
				},
			},
			want: nil,
		},
		{
			name: "CG owner reference",
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "Pod",
						Name:       "name",
					},
					{
						APIVersion: SchemeGroupVersion.String(),
						Kind:       ConsumerGroupGroupVersionKind.Kind,
						Name:       "cg-name",
					},
				},
			},
			want: &metav1.OwnerReference{
				APIVersion: SchemeGroupVersion.String(),
				Kind:       ConsumerGroupGroupVersionKind.Kind,
				Name:       "cg-name",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Consumer{ObjectMeta: tt.ObjectMeta}
			got := c.GetConsumerGroup()
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Error("(-want, +got)", diff)
			}
		})
	}
}
