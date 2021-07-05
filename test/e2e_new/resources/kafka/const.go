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

// TODO should these variables be configurable through envs?

const (
	// Kafka bootstrap server.
	BootstrapServersPlaintext     = "my-cluster-kafka-bootstrap.kafka:9092"
	BootstrapServersSsl           = "my-cluster-kafka-bootstrap.kafka:9093"
	BootstrapServersSaslPlaintext = "my-cluster-kafka-bootstrap.kafka:9094"
	BootstrapServersSslSaslScram  = "my-cluster-kafka-bootstrap.kafka:9095"
)

var (
	// Kafka bootstrap server as array.
	BootstrapServersPlaintextArray     = []string{BootstrapServersPlaintext}
	BootstrapServersSslArray           = []string{BootstrapServersSsl}
	BootstrapServersSaslPlaintextArray = []string{BootstrapServersSaslPlaintext}
	BootstrapServersSslSaslScramArray  = []string{BootstrapServersSslSaslScram}
)
