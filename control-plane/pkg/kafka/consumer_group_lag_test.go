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

package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsumerGroupLagTotal(t *testing.T) {
	tests := []struct {
		name string
		cgl  ConsumerGroupLag
		want uint64
	}{
		{
			name: "empty",
			cgl: ConsumerGroupLag{
				Topic:       "topic",
				ByPartition: []PartitionLag{},
			},
			want: 0,
		},
		{
			name: "consumer group lags",
			cgl: ConsumerGroupLag{
				Topic: "topic",
				ByPartition: []PartitionLag{
					{Lag: 20},
					{Lag: 21},
					{Lag: 22},
					{Lag: 1},
				},
			},
			want: 20 + 21 + 22 + 1,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cgl.Total(); got != tt.want {
				t.Errorf("Total() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConsumerGroupLagString(t *testing.T) {
	cgl := ConsumerGroupLag{}
	require.NotEmpty(t, cgl.String())
}
