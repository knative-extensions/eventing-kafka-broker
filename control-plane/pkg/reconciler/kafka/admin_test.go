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
	"io"
	"testing"

	"github.com/Shopify/sarama"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

func TestGetClusterAdmin(t *testing.T) {

	tests := []struct {
		name             string
		adminFunc        NewClusterAdminFunc
		bootstrapServers []string
		secOptions       security.ConfigOption
		wantErr          bool
	}{
		{
			name:             "security options error",
			adminFunc:        func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error) { return nil, nil },
			bootstrapServers: []string{"bs:9092"},
			secOptions:       func(config *sarama.Config) error { return io.EOF },
			wantErr:          true,
		},
		{
			name:             "admin func error",
			adminFunc:        func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error) { return nil, io.EOF },
			bootstrapServers: []string{"bs:9092"},
			secOptions:       func(config *sarama.Config) error { return nil },
			wantErr:          true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := GetClusterAdmin(tt.adminFunc, tt.bootstrapServers, tt.secOptions)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetClusterAdmin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
