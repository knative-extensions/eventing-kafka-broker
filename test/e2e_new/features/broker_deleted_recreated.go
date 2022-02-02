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
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/resources/svc"

	"knative.dev/eventing-kafka-broker/test/e2e_new/features/featuressteps"
	"knative.dev/eventing-kafka-broker/test/e2e_new/resources/configmap"
)

func BrokerDeletedRecreated() *feature.Feature {
	f := feature.NewFeatureNamed("broker deleted and recreated")

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")

	f.Setup("test broker", featuressteps.BrokerSmokeTest(brokerName, triggerName))
	f.Requirement("delete broker", featuressteps.DeleteBroker(brokerName))
	f.Assert("test broker after deletion", featuressteps.BrokerSmokeTest(brokerName, triggerName))

	return f
}

func BrokerConfigMapDeletedFirst() *feature.Feature {
	f := feature.NewFeatureNamed("delete broker ConfigMap first")

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")
	cmName := feature.MakeRandomK8sName("cm-deleted-first")
	sink := feature.MakeRandomK8sName("sink")

	f.Setup("install sink", eventshub.Install(sink, eventshub.StartReceiver))
	f.Setup("install config", configmap.Copy(
		types.NamespacedName{Namespace: system.Namespace(), Name: "kafka-broker-config"},
		cmName,
	))
	f.Setup("install broker", broker.Install(brokerName, append(broker.WithEnvConfig(), broker.WithConfig(cmName))...))
	f.Setup("install trigger", trigger.Install(triggerName, brokerName,
		trigger.WithSubscriber(svc.AsKReference(sink), "")),
	)

	f.Requirement("delete Broker ConfigMap", featuressteps.DeleteConfigMap(cmName))

	f.Assert("delete broker", featuressteps.DeleteBroker(brokerName))

	return f
}
