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

package kafkasink_test

import (
	"os"

	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/test/e2e_new/resources/kafkasink"

	"knative.dev/reconciler-test/pkg/manifest"
)

// The following examples validate the processing of the With* helper methods
// applied to config and go template parser.

func Example_zero() {
	images := map[string]string{}
	cfg := map[string]interface{}{
		"name":             "foo",
		"namespace":        "bar",
		"topic":            "my-topic",
		"bootstrapServers": []string{"my-bootstrap-server:8082"},
	}

	files, err := manifest.ExecuteLocalYAML(images, cfg)
	if err != nil {
		panic(err)
	}

	manifest.OutputYAML(os.Stdout, files)
	// Output:
	// apiVersion: eventing.knative.dev/v1alpha1
	// kind: KafkaSink
	// metadata:
	//   name: foo
	//   namespace: bar
	// spec:
	//   topic: "my-topic"
	//   bootstrapServers:
	//     - "my-bootstrap-server:8082"
}

func Example_full() {
	images := map[string]string{}
	cfg := map[string]interface{}{
		"name":             "foo",
		"namespace":        "bar",
		"topic":            "my-topic",
		"bootstrapServers": []string{"my-bootstrap-server:8082"},
	}
	kafkasink.WithContentMode(eventingv1alpha1.ModeBinary)(cfg)
	kafkasink.WithReplicationFactor(3)(cfg)
	kafkasink.WithNumPartitions(10)(cfg)
	kafkasink.WithAuthSecretName("abc")(cfg)

	files, err := manifest.ExecuteLocalYAML(images, cfg)
	if err != nil {
		panic(err)
	}

	manifest.OutputYAML(os.Stdout, files)
	// Output:
	// apiVersion: eventing.knative.dev/v1alpha1
	// kind: KafkaSink
	// metadata:
	//   name: foo
	//   namespace: bar
	// spec:
	//   topic: "my-topic"
	//   bootstrapServers:
	//     - "my-bootstrap-server:8082"
	//   contentMode: binary
	//   numPartitions: 10
	//   replicationFactor: 3
	//   auth:
	//     secret:
	//       ref:
	//         name: abc
}
