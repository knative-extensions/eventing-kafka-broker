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

package main

import (
	"log"

	"github.com/kelseyhightower/envconfig"
	"k8s.io/apimachinery/pkg/types"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	testobservability "knative.dev/eventing-kafka-broker/test/pkg/observability"
)

func main() {

	envConfig := &config.Env{}

	if err := envconfig.Process("", envConfig); err != nil {
		log.Fatal("failed to process env config", err)
	}

	testobservability.WatchDataPlaneConfigMap(
		types.NamespacedName{
			Namespace: envConfig.DataPlaneConfigMapNamespace,
			Name:      envConfig.DataPlaneConfigMapName,
		},
		envConfig.DataPlaneConfigFormat,
	)

}
