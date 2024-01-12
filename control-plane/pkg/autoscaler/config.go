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

package autoscaler

import (
	"fmt"
	"strconv"
)

// AutoscalerConfig contains the defaults defined in the autoscaler ConfigMap.
type AutoscalerConfig struct {
	AutoscalerClass    map[string]string
	AutoscalerDefaults map[string]int32
}

func defaultConfig() *AutoscalerConfig {
	return &AutoscalerConfig{
		AutoscalerClass: map[string]string{
			AutoscalingClassAnnotation: AutoscalingClassDisabledAnnotationValue,
		},
		AutoscalerDefaults: map[string]int32{
			AutoscalingMinScaleAnnotation:        DefaultMinReplicaCount,
			AutoscalingMaxScaleAnnotation:        DefaultMaxReplicaCount,
			AutoscalingPollingIntervalAnnotation: DefaultPollingInterval,
			AutoscalingCooldownPeriodAnnotation:  DefaultCooldownPeriod,
			AutoscalingLagThreshold:              DefaultLagThreshold,
		},
	}
}

// NewConfigFromMap creates a AutoscalerConfig from the supplied map
func NewConfigFromMap(data map[string]string) (*AutoscalerConfig, error) {
	ac := defaultConfig()

	if class, ok := data["class"]; ok && class == AutoscalingClassDisabledAnnotationValue { //autoscaler disabled - ignore new defaults
		return ac, nil
	}

	for key, val := range data {
		key = convertConfigKeyToAutoscalingAnnotation(key)

		if key == AutoscalingClassAnnotation {
			ac.AutoscalerClass[AutoscalingClassAnnotation] = val
		} else {
			i, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("Expected value for autoscaling annotation: "+key+" should be integer but got "+val, err)
			}
			ac.AutoscalerDefaults[key] = int32(i)
		}
	}

	return ac, nil
}

func convertConfigKeyToAutoscalingAnnotation(configkey string) (autoscalingannotation string) {
	return Autoscaling + "/" + configkey
}
