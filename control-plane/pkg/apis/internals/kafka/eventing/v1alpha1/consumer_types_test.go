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
	"io"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
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

func TestByReadinessAndCreationTime(t *testing.T) {

	consumers := []*Consumer{
		// Not ready, first created
		func() *Consumer {
			c := &Consumer{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "c1",
					Namespace:         "ns",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Second)},
				},
			}
			c.GetConditionSet().Manage(c.GetStatus()).InitializeConditions()
			return c
		}(),
		// Not ready, second created
		func() *Consumer {
			c := &Consumer{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "c2",
					Namespace:         "ns",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Minute)},
				},
			}
			c.GetConditionSet().Manage(c.GetStatus()).InitializeConditions()
			_ = c.MarkBindFailed(io.EOF)
			return c
		}(),
		// Ready, third created
		func() *Consumer {
			c := &Consumer{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "c3",
					Namespace:         "ns",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Hour)},
				},
			}
			c.GetConditionSet().Manage(c.GetStatus()).InitializeConditions()
			c.MarkBindSucceeded()
			c.MarkReconcileContractSucceeded()
			return c
		}(),
	}

	sort.Stable(ByReadinessAndCreationTime(consumers))

	require.Equal(t, 3, len(consumers))
	require.Equal(t, "c3", consumers[0].Name)
	require.Equal(t, "c1", consumers[1].Name)
	require.Equal(t, "c2", consumers[2].Name)
}
