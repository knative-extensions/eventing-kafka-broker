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

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
)

// ContentModeFromString returns the ContentMode from the given string.
func ContentModeFromString(mode string) contract.ContentMode {
	switch mode {
	case eventing.ModeBinary:
		return contract.ContentMode_BINARY
	case eventing.ModeStructured:
		return contract.ContentMode_STRUCTURED
	default:
		panic(fmt.Errorf(
			"unknown content mode: %s - allowed: %v",
			mode,
			[]string{eventing.ModeStructured, eventing.ModeBinary},
		))
	}
}
