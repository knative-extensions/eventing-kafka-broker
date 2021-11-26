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

package upgrade

import (
	"context"
	"testing"

	"knative.dev/eventing/test/e2e/helpers"

	testbroker "knative.dev/eventing-kafka-broker/test/pkg/broker"
	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func runBrokerSmokeTest(t *testing.T) {
	pkgtesting.RunMultiple(t, func(t *testing.T) {
		helpers.EventTransformationForTriggerTestHelper(
			context.Background(),
			t,
			/* broker version */ "v1",
			/* trigger version */ "v1",
			testbroker.Creator,
		)
	})
}
