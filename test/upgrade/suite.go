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

	"knative.dev/eventing-kafka-broker/test/upgrade/installation"
)

// Suite defines the whole upgrade test suite for Eventing Kafka.
func Suite() pkgupgrade.Suite {
	return pkgupgrade.Suite{
		Tests: pkgupgrade.Tests{
			PreUpgrade: []pkgupgrade.Operation{
				BrokerPreUpgradeTest(),
				ChannelPreUpgradeTest(),
				SinkPreUpgradeTest(),
			},
			PostUpgrade: []pkgupgrade.Operation{
				BrokerPostUpgradeTest(),
				//ChannelPostUpgradeTest(),
				SinkPostUpgradeTest(),
			},
			PostDowngrade: []pkgupgrade.Operation{
				BrokerPostDowngradeTest(),
				//ChannelPostDowngradeTest(),
				SinkPostDowngradeTest(),
			},
			Continual: ContinualTests(),
		},
		Installations: pkgupgrade.Installations{
			Base: []pkgupgrade.Operation{
				installation.LatestStable(),
			},
			UpgradeWith: []pkgupgrade.Operation{
				installation.GitHead(),
			},
			DowngradeWith: []pkgupgrade.Operation{
				installation.LatestStable(),
			},
		},
	}
}
