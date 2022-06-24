/*
 * Copyright 2020 The Knative Authors
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

package config

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

type Env struct {
	// DataPlaneConfigMapNamespace is the namespace of the configmap that holds the contract between the control plane
	// and the data plane. Might be ignored in some cases such as the configmap is expected to be in the resource
	// namespace
	DataPlaneConfigMapNamespace string `required:"true" split_words:"true"`

	// DataPlaneConfigMapName is the name of the configmap that holds the contract between the control plane
	// and the data plane.
	DataPlaneConfigMapName string `required:"true" split_words:"true"` // example: kafka-broker-triggers

	// GeneralConfigMapName is the name of the configmap that holds configuration that affects the control plane
	// and the data plane. For example, Kafka bootstrap server information could be in here for broker configuration.
	GeneralConfigMapName string `required:"true" split_words:"true"` // example: kafka-broker-config

	IngressName           string `required:"true" split_words:"true"` // example: kafka-broker-ingress
	IngressPodPort        string `required:"false" split_words:"true"`
	SystemNamespace       string `required:"true" split_words:"true"`
	DataPlaneConfigFormat string `required:"true" split_words:"true"`
	DefaultBackoffDelayMs uint64 `required:"false" split_words:"true"`

	// DispatcherImage is the dispatcher image that is used when the data plane is created dynamically
	DispatcherImage string `required:"false" split_words:"true"`

	// ReceiverImage is the receiver image that is used when the data plane is created dynamically
	ReceiverImage string `required:"false" split_words:"true"`
}

// ValidationOption represents a function to validate the Env configurations.
type ValidationOption func(env Env) error

func GetEnvConfig(prefix string, validations ...ValidationOption) (*Env, error) {
	env := &Env{}

	if err := envconfig.Process(prefix, env); err != nil {
		return nil, fmt.Errorf("failed to process env config: %w", err)
	}

	for _, v := range validations {
		if err := v(*env); err != nil {
			return nil, fmt.Errorf("env config validation failed: %w", err)
		}
	}

	return env, nil
}

func (c *Env) DataPlaneConfigMapAsString() string {
	return fmt.Sprintf("%s/%s", c.DataPlaneConfigMapNamespace, c.DataPlaneConfigMapName)
}
