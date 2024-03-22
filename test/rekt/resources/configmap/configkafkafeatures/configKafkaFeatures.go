/*
 * Copyright 2024 The Knative Authors
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

package configkafkafeatures

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"

	"knative.dev/reconciler-test/pkg/feature"
)

const configKafkaFeatures = "config-kafka-features"

type CfgFn func(cfg map[string]string)

func Install(opts ...CfgFn) feature.StepFn {
	cfg := map[string]string{}

	for _, fn := range opts {
		fn(cfg)
	}

	return func(ctx context.Context, t feature.T) {
		client := kubeclient.Get(ctx)
		ns := system.Namespace()
		cm, err := client.CoreV1().ConfigMaps(ns).Get(ctx, configKafkaFeatures, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get configmap config-kafka-features: %s", err.Error())
		}

		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}

		for k, v := range cfg {
			cm.Data[k] = v
		}

		_, err = client.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("failed to update configmap config-kafka-features: %s", err.Error())
		}
	}
}

func WithTriggerConsumerGroupIDTemplate(template string) CfgFn {
	return func(cfg map[string]string) {
		cfg["triggers.consumergroup.template"] = template
	}
}
