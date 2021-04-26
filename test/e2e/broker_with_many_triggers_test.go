// +build e2e

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

package e2e

import (
	"context"
	"testing"

	"knative.dev/eventing/test/e2e/helpers"

	testbroker "knative.dev/eventing-kafka-broker/test/pkg/broker"
)

func TestBrokerWithManyTriggers(t *testing.T) {
	t.Skip("Pass more events than necessary (related? https://knative.slack.com/archives/C9JP909F0/p1595244377489600)")

	helpers.TestBrokerWithManyTriggers(context.Background(), t, testbroker.Creator, false)
}
