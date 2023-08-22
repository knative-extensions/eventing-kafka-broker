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

package consumergroup

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler"
)

func (r *Reconciler) autoscalerDefaultsFromConfigMap(ctx context.Context, configMapName string) (*autoscaler.AutoscalerConfig, error) {
	cm, err := r.KubeClient.CoreV1().ConfigMaps(r.SystemNamespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error while retrieving the %s config map in namespace %s: %+v", configMapName, r.SystemNamespace, err)
	}

	config, err := autoscaler.NewConfigFromMap(cm.Data)
	if err != nil {
		return nil, fmt.Errorf("error while parsing the %s config map in namespace %s: %+v", configMapName, r.SystemNamespace, err)
	}

	return config, nil
}
