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

package featuressteps

import (
	"context"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/resources/svc"
)

func compose(steps ...feature.StepFn) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		for _, s := range steps {
			s(ctx, t)
		}
	}
}

func BrokerSmokeTest(brokerName, triggerName string) feature.StepFn {

	sink := feature.MakeRandomK8sName("sink")

	event := cetest.FullEvent()
	event.SetID(uuid.New().String())

	eventMatchers := []cetest.EventMatcher{
		cetest.HasId(event.ID()),
		cetest.HasSource(event.Source()),
		cetest.HasType(event.Type()),
		cetest.HasSubject(event.Subject()),
	}

	backoffPolicy := eventingduck.BackoffPolicyLinear

	return compose(
		eventshub.Install(sink, eventshub.StartReceiver),
		broker.Install(brokerName, broker.WithEnvConfig()...),
		broker.IsReady(brokerName),
		trigger.Install(
			triggerName,
			brokerName,
			trigger.WithRetry(3, &backoffPolicy, pointer.StringPtr("PT1S")),
			trigger.WithSubscriber(svc.AsKReference(sink), ""),
		),
		trigger.IsReady(triggerName),
		eventshub.Install(
			feature.MakeRandomK8sName("source"),
			eventshub.StartSenderToResource(broker.GVR(), brokerName),
			eventshub.AddSequence,
			eventshub.AddTracing,
			eventshub.InputEvent(event),
		),
		assert.OnStore(sink).MatchEvent(eventMatchers...).Exact(1),
	)
}

func DeleteResources(f *feature.Feature) feature.StepFn {
	return compose(f.DeleteResources)
}
