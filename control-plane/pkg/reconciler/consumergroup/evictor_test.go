/*
 * Copyright 2022 The Knative Authors
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

package consumergroup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	filteredFactory "knative.dev/pkg/client/injection/kube/informers/factory/filtered"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
	kafkainternalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
)

func TestNewEvictor(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t, withFilteredSelectors)

	require.NotPanics(t, func() { newEvictor(ctx, zap.String("k", "n")) })
}

func TestEvictorNilPodNoPanic(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t, withFilteredSelectors)

	var pod *corev1.Pod

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "name1"},
	}

	cg := &kafkainternals.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "cg-name"},
		Status: kafkainternals.ConsumerGroupStatus{
			PlaceableStatus: eventingduckv1alpha1.PlaceableStatus{Placeable: eventingduckv1alpha1.Placeable{
				MaxAllowedVReplicas: pointer.Int32(1),
				Placements: []eventingduckv1alpha1.Placement{
					{PodName: "name", VReplicas: 1},
					{PodName: pod1.GetName(), VReplicas: 1},
				},
			}},
		},
	}
	cg.GetConditionSet().Manage(cg.GetStatus()).InitializeConditions()
	cg.MarkScheduleSucceeded()

	placement := &eventingduckv1alpha1.Placement{PodName: pod1.GetName(), VReplicas: 1}

	ctx, _ = kafkainternalsclient.With(ctx, cg)

	e := newEvictor(ctx)
	err := e.evict(pod, cg, placement)

	require.Nil(t, err)
}

func TestEvictorEvictSuccess(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t, withFilteredSelectors)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "name"},
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "name1"},
	}

	cg := &kafkainternals.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "cg-name"},
		Status: kafkainternals.ConsumerGroupStatus{
			PlaceableStatus: eventingduckv1alpha1.PlaceableStatus{Placeable: eventingduckv1alpha1.Placeable{
				MaxAllowedVReplicas: pointer.Int32(1),
				Placements: []eventingduckv1alpha1.Placement{
					{PodName: pod.GetName(), VReplicas: 1},
					{PodName: pod1.GetName(), VReplicas: 1},
				},
			}},
		},
	}
	cg.GetConditionSet().Manage(cg.GetStatus()).InitializeConditions()
	cg.MarkScheduleSucceeded()

	placement := &eventingduckv1alpha1.Placement{PodName: pod1.GetName(), VReplicas: 1}

	ctx, _ = kubeclient.With(ctx, pod)
	ctx, _ = kafkainternalsclient.With(ctx, cg)

	e := newEvictor(ctx)
	err := e.evict(pod, cg, placement)

	require.Nil(t, err)
}

func TestEvictorNoEvictionEmptyPlacement(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t, withFilteredSelectors)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "name"},
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "name1"},
	}

	cg := &kafkainternals.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "cg-name"},
		Status: kafkainternals.ConsumerGroupStatus{
			PlaceableStatus: eventingduckv1alpha1.PlaceableStatus{Placeable: eventingduckv1alpha1.Placeable{
				MaxAllowedVReplicas: pointer.Int32(1),
				Placements:          []eventingduckv1alpha1.Placement{},
			}},
		},
	}
	cg.GetConditionSet().Manage(cg.GetStatus()).InitializeConditions()
	cg.MarkScheduleSucceeded()

	placement := &eventingduckv1alpha1.Placement{PodName: pod1.GetName(), VReplicas: 1}

	ctx, _ = kubeclient.With(ctx, pod)
	ctx, _ = kafkainternalsclient.With(ctx, cg)

	e := newEvictor(ctx)
	err := e.evict(pod, cg, placement)

	require.Nil(t, err)
}

func TestEvictorNoEviction(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t, withFilteredSelectors)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "name"},
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "name1"},
	}

	cg := &kafkainternals.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "cg-name"},
		Status: kafkainternals.ConsumerGroupStatus{
			PlaceableStatus: eventingduckv1alpha1.PlaceableStatus{Placeable: eventingduckv1alpha1.Placeable{
				MaxAllowedVReplicas: pointer.Int32(1),
				Placements: []eventingduckv1alpha1.Placement{
					{PodName: pod.GetName(), VReplicas: 1},
				},
			}},
		},
	}
	cg.GetConditionSet().Manage(cg.GetStatus()).InitializeConditions()
	cg.MarkScheduleSucceeded()

	placement := &eventingduckv1alpha1.Placement{PodName: pod1.GetName(), VReplicas: 1}

	ctx, _ = kubeclient.With(ctx, pod)
	ctx, _ = kafkainternalsclient.With(ctx, cg)

	e := newEvictor(ctx)
	err := e.evict(pod, cg, placement)

	require.Nil(t, err)
}

func TestEvictorEvictSuccessConsumerGroupSchedulingInProgress(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t, withFilteredSelectors)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "name"},
	}
	cg := &kafkainternals.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "cg-name"},
		Status: kafkainternals.ConsumerGroupStatus{
			PlaceableStatus: eventingduckv1alpha1.PlaceableStatus{Placeable: eventingduckv1alpha1.Placeable{
				MaxAllowedVReplicas: pointer.Int32(1),
				Placements: []eventingduckv1alpha1.Placement{
					{PodName: pod.GetName(), VReplicas: 1},
				},
			}},
		},
	}
	cg.GetConditionSet().Manage(cg.GetStatus()).InitializeConditions()

	placement := &eventingduckv1alpha1.Placement{PodName: pod.GetName(), VReplicas: 1}

	ctx, _ = kubeclient.With(ctx, pod)
	ctx, _ = kafkainternalsclient.With(ctx, cg)

	e := newEvictor(ctx)
	err := e.evict(pod, cg, placement)

	require.True(t, cg.IsNotScheduled())
	require.Nil(t, err)
}

func TestEvictorEvictPodNotFound(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t, withFilteredSelectors)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "name"},
	}
	cg := &kafkainternals.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "cg-name"},
		Status: kafkainternals.ConsumerGroupStatus{
			PlaceableStatus: eventingduckv1alpha1.PlaceableStatus{Placeable: eventingduckv1alpha1.Placeable{
				MaxAllowedVReplicas: pointer.Int32(1),
				Placements: []eventingduckv1alpha1.Placement{
					{PodName: pod.GetName(), VReplicas: 1},
				},
			}},
		},
	}
	placement := &eventingduckv1alpha1.Placement{PodName: pod.GetName(), VReplicas: 1}

	ctx, _ = kafkainternalsclient.With(ctx, cg)

	e := newEvictor(ctx)
	err := e.evict(pod, cg, placement)

	require.Nil(t, err)
}
func TestEvictorEvictConsumerGroupNotFound(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t, withFilteredSelectors)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "name"},
	}
	cg := &kafkainternals.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "cg-name"},
		Status: kafkainternals.ConsumerGroupStatus{
			PlaceableStatus: eventingduckv1alpha1.PlaceableStatus{Placeable: eventingduckv1alpha1.Placeable{
				MaxAllowedVReplicas: pointer.Int32(1),
				Placements: []eventingduckv1alpha1.Placement{
					{PodName: pod.GetName(), VReplicas: 1},
				},
			}},
		},
	}
	placement := &eventingduckv1alpha1.Placement{PodName: pod.GetName(), VReplicas: 1}

	ctx, _ = kubeclient.With(ctx, pod)

	e := newEvictor(ctx)
	err := e.evict(pod, cg, placement)

	require.Nil(t, err)
}

func withFilteredSelectors(ctx context.Context) context.Context {
	return filteredFactory.WithSelectors(ctx,
		eventing.DispatcherLabelSelectorStr,
	)
}
