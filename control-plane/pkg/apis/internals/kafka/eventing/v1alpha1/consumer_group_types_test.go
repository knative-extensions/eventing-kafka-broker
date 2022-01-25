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

func TestConsumerGroup_GetUserFacingResourceRef(t *testing.T) {
	tests := []struct {
		name       string
		ObjectMeta metav1.ObjectMeta
		want       *metav1.OwnerReference
	}{
		{
			name:       "no owner ref",
			ObjectMeta: metav1.ObjectMeta{},
			want:       nil,
		},
		{
			name: "no known owner ref",
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
			name: "trigger owner ref",
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "Pod",
						Name:       "name",
					},
					{
						APIVersion: "eventing.knative.dev/v1",
						Kind:       "Trigger",
						Name:       "name",
					},
				},
			},
			want: &metav1.OwnerReference{
				APIVersion: "eventing.knative.dev/v1",
				Kind:       "Trigger",
				Name:       "name",
			},
		},
		{
			name: "kafkasource owner ref",
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "Pod",
						Name:       "name",
					},
					{
						APIVersion: "sources.knative.dev/v1",
						Kind:       "KafkaSource",
						Name:       "name",
					},
				},
			},
			want: &metav1.OwnerReference{
				APIVersion: "sources.knative.dev/v1",
				Kind:       "KafkaSource",
				Name:       "name",
			},
		},
		{
			name: "kafkachannel owner ref",
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "Pod",
						Name:       "name",
					},
					{
						APIVersion: "messaging.knative.dev/v1beta1",
						Kind:       "KafkaChannel",
						Name:       "name",
					},
				},
			},
			want: &metav1.OwnerReference{
				APIVersion: "messaging.knative.dev/v1beta1",
				Kind:       "KafkaChannel",
				Name:       "name",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cg := &ConsumerGroup{ObjectMeta: tt.ObjectMeta}
			got := cg.GetUserFacingResourceRef()
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Error("(-want, +got)", diff)
			}
		})
	}
}
