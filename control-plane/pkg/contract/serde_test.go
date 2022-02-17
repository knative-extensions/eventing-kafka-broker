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

package contract_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
)

func TestFormatSerDe(t *testing.T) {

	tt := []struct {
		serde contract.FormatSerDe
	}{
		{
			serde: contract.FormatSerDe{Format: contract.Protobuf},
		},
		{
			serde: contract.FormatSerDe{Format: contract.Json},
		},
	}

	for _, tc := range tt {
		t.Run(string(tc.serde.Format), func(t *testing.T) {
			ct := &contract.Contract{
				Generation: 42,
				Resources: []*contract.Resource{
					{Uid: "abc"},
					{Uid: "abd"},
				},
			}

			b, err := tc.serde.Serialize(ct)
			require.Nil(t, err)

			got, err := tc.serde.Deserialize(b)
			require.Nil(t, err)

			require.True(t, proto.Equal(ct, got), "%v %v", ct, got)
		})
	}
}
