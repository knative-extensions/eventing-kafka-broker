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
	pkgupgrade "knative.dev/pkg/test/upgrade"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/test/e2e"
)

// BrokerPreUpgradeTest tests channel operations before upgrade.
func BrokerPreUpgradeTest() pkgupgrade.Operation {
	return pkgupgrade.NewOperation("BrokerPreUpgradeTest", func(c pkgupgrade.Context) {
		runBrokerSmokeTest(c.T)
	})
}

// SinkPreUpgradeTest tests sink basic operations pre upgrade.
func SinkPreUpgradeTest() pkgupgrade.Operation {
	return pkgupgrade.NewOperation("SinkPreUpgradeTest", func(c pkgupgrade.Context) {
		e2e.RunTestKafkaSink(c.T, eventing.ModeBinary, nil)
	})
}
