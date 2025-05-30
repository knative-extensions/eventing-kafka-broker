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

package pkg

import (
	"fmt"
	"testing"
)

const (
	// Number of times to run a test function.
	rerunTimes = 5

	// Kafka bootstrap server.
	BootstrapServersPlaintext              = "my-cluster-kafka-bootstrap.kafka:9092"
	BootstrapServersSsl                    = "my-cluster-kafka-bootstrap.kafka:9093"
	BootstrapServersTlsNoAuth              = "my-cluster-kafka-bootstrap.kafka:9096"
	BootstrapServersSaslPlaintext          = "my-cluster-kafka-bootstrap.kafka:9095"
	BootstrapServersSslSaslScram           = "my-cluster-kafka-bootstrap.kafka:9094"
	BootstrapServersImplicitCASslSaslScram = "my-cluster-kafka-bootstrap.kafka:9194"

	NumPartitions     = 10
	ReplicationFactor = 3

	KafkaClusterNamespace        = "kafka"
	TlsUserSecretName            = "my-tls-user"
	SaslUserSecretName           = "my-sasl-user"
	SaslRestrictedUserSecretName = "my-restricted-sasl-user"
	CaSecretName                 = "my-cluster-cluster-ca-cert"
)

var (
	// Kafka bootstrap server as array.
	BootstrapServersPlaintextArr     = []string{BootstrapServersPlaintext}
	BootstrapServersSslArr           = []string{BootstrapServersSsl}
	BootstrapServersTlsNoAuthArr     = []string{BootstrapServersTlsNoAuth}
	BootstrapServersSaslPlaintextArr = []string{BootstrapServersSaslPlaintext}
	BootstrapServersSslSaslScramArr  = []string{BootstrapServersSslSaslScram}
)

// RunMultiple run test function f `rerunTimes` times.
func RunMultiple(t *testing.T, f func(t *testing.T)) {
	RunMultipleN(t, rerunTimes, f)
}

// RunMultiple run test function f n times.
func RunMultipleN(t *testing.T, n int, f func(t *testing.T)) {
	t.Parallel()

	if testing.Short() {
		n = 1
	}

	for i := 0; i < n; i++ {
		t.Run(fmt.Sprintf("%d", i), f)
	}
}
