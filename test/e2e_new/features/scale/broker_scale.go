/*
 * Copyright 2022 The Knative Authors
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

package scale

import (
	"fmt"
	"time"

	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
)

func BrokerTriggerLimits() *feature.Feature {
	f := feature.NewFeatureNamed("Broker and Trigger limits")

	timings := []time.Duration{20 * time.Second, 20 * time.Minute}

	nBrokers := 50
	nTriggers := 5
	exponential := eventingduck.BackoffPolicyExponential

	for b := 0; b < nBrokers; b++ {
		cfg := broker.WithEnvConfig()
		cfg = append(cfg, broker.WithRetry(10, &exponential, pointer.String("PT0.2S")))
		cfg = append(cfg, broker.WithDeadLetterSink(nil, "https://my-sink.com"))

		f.Setup("Install broker "+brokerName(b), broker.Install(brokerName(b), cfg...))
		f.Assert("Broker "+brokerName(b)+" is ready", broker.IsReady(brokerName(b), timings...))

		for i := 0; i < nTriggers; i++ {
			cfg := []manifest.CfgFn{
				trigger.WithFilter(map[string]string{
					"type":    "dev.knative.api.webhook",
					"subject": "ping",
				}),
				trigger.WithRetry(10, &exponential, pointer.String("PT0.2S")),
				trigger.WithDeadLetterSink(nil, "https://example.com"),
				trigger.WithSubscriber(nil, "https://example.com"),
			}
			f.Setup("Install trigger "+triggerName(b, i), trigger.Install(triggerName(b, i), brokerName(b), cfg...))
			f.Assert("Trigger "+triggerName(b, i)+" is ready", trigger.IsReady(triggerName(b, i), timings...))
		}
	}

	f.Teardown("Delete resources", f.DeleteResources)

	return f
}

func brokerName(i int) string {
	return fmt.Sprintf("broker-%d", i)
}

func triggerName(a, b int) string {
	return fmt.Sprintf("trigger-%d-%d", a, b)
}
