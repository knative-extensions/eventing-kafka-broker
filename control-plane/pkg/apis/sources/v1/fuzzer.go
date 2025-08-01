/*
Copyright 2020 The Knative Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	pkgfuzzer "knative.dev/pkg/apis/testing/fuzzer"
	"sigs.k8s.io/randfill"
)

// FuzzerFuncs includes fuzzing funcs for sources.knative.dev v1 types
//
// For other examples see
// https://github.com/kubernetes/apimachinery/blob/master/pkg/apis/meta/fuzzer/fuzzer.go
var FuzzerFuncs = fuzzer.MergeFuzzerFuncs(
	func(codecs serializer.CodecFactory) []interface{} {
		return []interface{}{
			func(s *KafkaSourceStatus, c randfill.Continue) {
				c.FillNoCustom(s) // fuzz the status object

				// Clear the random fuzzed condition
				s.SetConditions(nil)

				// Fuzz the known conditions except their type value
				s.InitializeConditions()
				pkgfuzzer.FuzzConditions(&s.Status, c)
			},
		}
	},
)
