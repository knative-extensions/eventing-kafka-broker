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
	"fmt"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing/pkg/scheduler"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"

	internalv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned/typed/eventing/v1alpha1"
	kafkainternalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
)

type evictor struct {
	ctx context.Context

	kubeClient      kubernetes.Interface
	InternalsClient internalv1alpha1.InternalV1alpha1Interface

	logger *zap.Logger
}

// newEvictor creates a new evictor.
//
// fields are additional logger fields to be attached to the evictor logger.
func newEvictor(ctx context.Context, fields ...zap.Field) evictor {
	return evictor{
		ctx:             ctx,
		kubeClient:      kubeclient.Get(ctx),
		InternalsClient: kafkainternalsclient.Get(ctx).InternalV1alpha1(),
		logger: logging.FromContext(ctx).
			Desugar().
			With(zap.String("component", "evictor")).
			With(fields...),
	}
}

func (e evictor) evict(pod *corev1.Pod, vpod scheduler.VPod, from *eventingduckv1alpha1.Placement) error {
	key := vpod.GetKey()

	logger := e.logger.
		With(zap.String("consumergroup", key.String())).
		With(zap.String("pod", fmt.Sprintf("%s/%s", pod.GetNamespace(), pod.GetName())))

	if err := e.disablePodScheduling(logger, pod.DeepCopy() /* Do not modify informer copy. */); err != nil {
		return fmt.Errorf("failed to mark pod unschedulable: %w", err)
	}

	cg, err := e.InternalsClient.
		ConsumerGroups(key.Namespace).
		Get(e.ctx, key.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get consumer group %s/%s: %w", key.Namespace, key.Name, err)
	}

	// Do not evict when the consumer group isn't scheduled yet.
	if cg.IsNotScheduled() {
		return nil
	}

	cg.Status.Placements = removePlacement(cg.GetPlacements(), from)

	_, err = e.InternalsClient.
		ConsumerGroups(cg.GetNamespace()).
		Update(e.ctx, cg, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to update consumer group %s/%s: %w", cg.GetNamespace(), cg.GetName(), err)
	}

	return nil
}

func (e *evictor) disablePodScheduling(logger *zap.Logger, pod *corev1.Pod) error {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string, 1)
	}
	// scheduling disabled.
	pod.Annotations[scheduler.PodAnnotationKey] = "true"

	_, err := e.kubeClient.CoreV1().
		Pods(pod.GetNamespace()).
		Update(e.ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update pod %s/%s: %w", pod.GetNamespace(), pod.GetName(), err)
	}

	logger.Info("Marked pod as unschedulable")

	return nil
}

func removePlacement(before []eventingduckv1alpha1.Placement, toRemove *eventingduckv1alpha1.Placement) []eventingduckv1alpha1.Placement {
	after := make([]eventingduckv1alpha1.Placement, 0, len(before))
	for _, p := range before {
		if p.PodName != toRemove.PodName {
			after = append(after, p)
		}
	}
	return after
}
