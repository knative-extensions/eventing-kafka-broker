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

package features

import (
	"knative.dev/reconciler-test/pkg/feature"

	"knative.dev/eventing-kafka-broker/test/e2e_new/features/featuressteps"
)

func BrokerDeletedRecreated() *feature.Feature {
	f := feature.NewFeatureNamed("broker deleted and recreated")

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")

	f.Setup("test broker", featuressteps.BrokerSmokeTest(brokerName, triggerName))
	f.Requirement("delete resources", featuressteps.DeleteResources(f))
	f.Assert("test broker after deletion", featuressteps.BrokerSmokeTest(brokerName, triggerName))

	return f
}
