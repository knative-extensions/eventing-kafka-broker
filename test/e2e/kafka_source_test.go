//go:build e2e
// +build e2e

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

package e2e

import (
	"testing"

	eventingkafkahelpers "knative.dev/eventing-kafka/test/e2e/helpers"

	testingpkg "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func TestKafkaSourceUpdate(t *testing.T) {
	testingpkg.RunMultiple(t, eventingkafkahelpers.TestKafkaSourceUpdate)
}

func TestKafkaSourceAssureIsOperational(t *testing.T) {
	testingpkg.RunMultiple(t, func(t *testing.T) {
		eventingkafkahelpers.AssureKafkaSourceIsOperational(t, func(auth, testCase, version string) bool { return true })
	})
}
