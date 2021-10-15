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
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/types"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
)

const (
	NoEgress = NoResource
)

// FindEgress finds the egress with the given UID in the given egresses list.
func FindEgress(egresses []*contract.Egress, egress types.UID) int {

	for i, t := range egresses {
		if t.Uid == string(egress) {
			return i
		}
	}

	return NoEgress
}

const (
	EgressChanged = iota
	EgressUnchanged
)

// AddOrUpdateEgressConfig adds or updates the given egress to the given contract at the specified indexes.
func AddOrUpdateEgressConfig(ct *contract.Contract, brokerIndex int, egress *contract.Egress, egressIndex int) int {

	if egressIndex != NoEgress {
		prev := ct.Resources[brokerIndex].Egresses[egressIndex]
		ct.Resources[brokerIndex].Egresses[egressIndex] = egress

		if proto.Equal(prev, egress) {
			return EgressUnchanged
		}
		return EgressChanged
	}

	ct.Resources[brokerIndex].Egresses = append(
		ct.Resources[brokerIndex].Egresses,
		egress,
	)

	return EgressChanged
}

func KeyTypeFromString(s string) contract.KeyType {
	switch s {
	case "byte-array":
		return contract.KeyType_ByteArray
	case "string":
		return contract.KeyType_String
	case "int":
		return contract.KeyType_Integer
	case "float":
		return contract.KeyType_Double
	default:
		return contract.KeyType_String
	}
}
