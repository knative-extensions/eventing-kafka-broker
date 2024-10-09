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
	"slices"

	"knative.dev/eventing/test/upgrade"
	pkgupgrade "knative.dev/pkg/test/upgrade"
	"knative.dev/reconciler-test/pkg/environment"

	"knative.dev/eventing-kafka-broker/test/upgrade/installation"
)

// Suite defines the whole upgrade test suite for Eventing Kafka.
func Suite(glob environment.GlobalEnvironment) pkgupgrade.Suite {
	g := upgrade.FeatureGroupWithUpgradeTests{
		// Features that will run the same test post-upgrade and post-downgrade.
		upgrade.NewFeatureSmoke(KafkaSinkSourceBinaryEventFeature(glob)),
		upgrade.NewFeatureSmoke(KafkaSinkSourceStructuredEventFeature(glob)),
		upgrade.NewFeatureSmoke(BrokerEventTransformationForTrigger(glob)),
		upgrade.NewFeatureSmoke(NamespacedBrokerEventTransformationForTrigger(glob)),
		upgrade.NewFeatureSmoke(KafkaChannelFeature(glob)),
		// Features that will be created pre-upgrade and verified/removed post-upgrade.
		upgrade.NewFeatureOnlyUpgrade(KafkaSinkSourceBinaryEventFeature(glob)),
		upgrade.NewFeatureOnlyUpgrade(KafkaSinkSourceStructuredEventFeature(glob)),
		upgrade.NewFeatureOnlyUpgrade(BrokerEventTransformationForTrigger(glob)),
		upgrade.NewFeatureOnlyUpgrade(NamespacedBrokerEventTransformationForTrigger(glob)),
		upgrade.NewFeatureOnlyUpgrade(KafkaChannelFeature(glob)),
		// Features that will be created pre-upgrade, verified post-upgrade, verified and removed post-downgrade.
		upgrade.NewFeatureUpgradeDowngrade(KafkaSinkSourceBinaryEventFeature(glob)),
		upgrade.NewFeatureUpgradeDowngrade(KafkaSinkSourceStructuredEventFeature(glob)),
		upgrade.NewFeatureUpgradeDowngrade(BrokerEventTransformationForTrigger(glob)),
		upgrade.NewFeatureUpgradeDowngrade(NamespacedBrokerEventTransformationForTrigger(glob)),
		upgrade.NewFeatureUpgradeDowngrade(KafkaChannelFeature(glob)),
		// Features that will be created post-upgrade, verified and removed post-downgrade.
		upgrade.NewFeatureOnlyDowngrade(KafkaSinkSourceBinaryEventFeature(glob)),
		upgrade.NewFeatureOnlyDowngrade(KafkaSinkSourceStructuredEventFeature(glob)),
		upgrade.NewFeatureOnlyDowngrade(BrokerEventTransformationForTrigger(glob)),
		upgrade.NewFeatureOnlyDowngrade(NamespacedBrokerEventTransformationForTrigger(glob)),
		upgrade.NewFeatureOnlyDowngrade(KafkaChannelFeature(glob)),
	}
	return pkgupgrade.Suite{
		Tests: pkgupgrade.Tests{
			PreUpgrade: g.PreUpgradeTests(),
			PostUpgrade: slices.Concat(
				[]pkgupgrade.Operation{
					VerifyPostInstallTest(),
				},
				g.PostUpgradeTests(),
			),
			PostDowngrade: g.PostDowngradeTests(),
			Continual:     ContinualTests(),
		},
		Installations: pkgupgrade.Installations{
			Base: []pkgupgrade.Operation{
				installation.LatestStable(glob),
			},
			UpgradeWith: []pkgupgrade.Operation{
				installation.GitHead(),
			},
			DowngradeWith: []pkgupgrade.Operation{
				installation.LatestStable(glob),
			},
		},
	}
}
