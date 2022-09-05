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

package kafka

import (
	"fmt"

	"github.com/magiconair/properties"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// IsOffsetLatest returns whether the configured `auto.offset.reset` it set to latest in the given ConfigMap.
func IsOffsetLatest(lister corelisters.ConfigMapLister, namespace, name, key string) (bool, error) {
	cm, err := lister.ConfigMaps(namespace).Get(name)
	if err != nil {
		return false, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, name, err)
	}

	consumerConfig, ok := cm.Data[key]
	if !ok {
		// latest by default
		return true, nil
	}

	consumerConfigProps, err := properties.LoadString(consumerConfig)
	if err != nil {
		// latest by default
		return true, fmt.Errorf("failed to read consumer configurations: %w", err)
	}

	offsetResetPolicy, ok := consumerConfigProps.Get("auto.offset.reset")
	if !ok {
		return true, nil
	}

	return offsetResetPolicy == "latest", nil
}
