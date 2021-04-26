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

package broker

import (
	"fmt"
	"strings"

	eventingtestlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/resources"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

func Creator(client *eventingtestlib.Client, version string) string {
	name := "broker"

	version = strings.ToLower(version)

	switch version {
	case "v1":
		client.CreateBrokerOrFail(
			name,
			resources.WithBrokerClassForBroker(kafka.BrokerClass),
		)
	default:
		panic(fmt.Sprintf("Unsupported version of Broker: %q", version))
	}

	return name
}
