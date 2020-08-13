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
	"encoding/json"
	"testing"

	"github.com/gogo/protobuf/proto"

	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
)

// run benchmark: go test -v -count=5 -cpu=2,4,8 -bench=. -benchmem > configmap_parsing_bench_test_out.txt

func BenchmarkJsonMarshal(b *testing.B) {
	brokers := getConfigMapData(getBroker(), getTrigger(), 25, 25)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(brokers)
	}
}

func BenchmarkProtoMarshal(b *testing.B) {
	brokers := getConfigMapData(getBroker(), getTrigger(), 25, 25)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(&brokers)
	}
}

func BenchmarkJsonUnmarshal(b *testing.B) {
	brokers := getConfigMapData(getBroker(), getTrigger(), 25, 25)
	data, _ := json.Marshal(brokers)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b := &coreconfig.Brokers{}
		_ = json.Unmarshal(data, b)
	}
}

func BenchmarkProtoUnmarshal(b *testing.B) {
	brokers := getConfigMapData(getBroker(), getTrigger(), 25, 25)
	data, _ := proto.Marshal(&brokers)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b := &coreconfig.Brokers{}
		_ = proto.Unmarshal(data, b)
	}
}
