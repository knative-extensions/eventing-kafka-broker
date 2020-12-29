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
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/types"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
)

const (
	// NoResource signals that the broker hasn't been found.
	NoResource = -1
)

// FindResource finds the resource with the given UID in the given resources list.
func FindResource(contract *contract.Contract, resource types.UID) int {
	resourceIndex := NoResource
	for i, b := range contract.Resources {
		if b.Uid == string(resource) {
			resourceIndex = i
			break
		}
	}
	return resourceIndex
}

const (
	ResourceChanged = iota
	ResourceUnchanged
)

// AddOrUpdateResourceConfig adds or updates the given resourceConfig to the given resources at the specified index.
func AddOrUpdateResourceConfig(contract *contract.Contract, resource *contract.Resource, index int, logger *zap.Logger) int {

	if index != NoResource {
		logger.Debug("Resource exists", zap.Int("index", index))

		prev := contract.Resources[index]
		resource.Egresses = contract.Resources[index].Egresses
		contract.Resources[index] = resource

		if proto.Equal(prev, resource) {
			return ResourceUnchanged
		}
		return ResourceChanged
	}

	logger.Debug("Resource doesn't exist")

	contract.Resources = append(contract.Resources, resource)

	return ResourceChanged
}

// DeleteResource deletes the resource at the given index from Resources.
func DeleteResource(ct *contract.Contract, index int) {

	if len(ct.Resources) == 1 {
		*ct = contract.Contract{
			Generation: ct.Generation,
		}
		return
	}

	// replace the resource to be deleted with the last one.
	ct.Resources[index] = ct.Resources[len(ct.Resources)-1]
	// truncate the array.
	ct.Resources = ct.Resources[:len(ct.Resources)-1]
}
