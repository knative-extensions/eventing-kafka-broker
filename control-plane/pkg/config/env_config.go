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
	DataPlaneConfigMapNamespace string `required:"true" split_words:"true"`
	DataPlaneConfigMapName      string `required:"true" split_words:"true"`
	GeneralConfigMapName        string `required:"true" split_words:"true"`
	IngressName                 string `required:"true" split_words:"true"`
	SystemNamespace             string `required:"true" split_words:"true"`
	DataPlaneConfigFormat       string `required:"true" split_words:"true"`
	DefaultBackoffDelayMs       uint64 `required:"false" split_words:"true"`
	ProbeFailureRequeueDelayMs  int64  `required:"false" split_words:"true" default:"50"`
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
