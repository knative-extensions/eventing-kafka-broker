/*
 * Copyright 2023 The Knative Authors
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

package kafkafeatureflags

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkafeature "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/feature"
)

func AutoscalingEnabled() feature.ShouldRun {
	return func(ctx context.Context, t feature.T) (feature.PrerequisiteResult, error) {
		flags, err := getKafkaFeatureFlags(ctx, "config-kafka-features")
		if err != nil {
			return feature.PrerequisiteResult{}, err
		}

		return feature.PrerequisiteResult{
			ShouldRun: flags.IsControllerAutoscalerEnabled(),
		}, nil

	}
}

func getKafkaFeatureFlags(ctx context.Context, cmName string) (*kafkafeature.KafkaFeatureFlags, error) {
	ns := system.Namespace()
	cm, err := kubeclient.Get(ctx).
		CoreV1().
		ConfigMaps(ns).
		Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get cm %s/%s: %s", ns, cmName, err)
	}

	return kafkafeature.NewFeaturesConfigFromMap(cm)
}
