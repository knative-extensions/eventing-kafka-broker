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

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/olekukonko/tablewriter"

	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	brokerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
)

const (
	// longest identifier: 253 -> https://github.com/kubernetes/kubernetes/blob/release-0.19/docs/design/identifiers.md
	// maximum bytes storable in etcd: 1MB -> 1048576

	longestName          = 253
	longestBrokerName    = longestName
	longestNamespaceName = longestName
	k8sServiceSuffix     = "svc.cluster.local" // usual case
)

func main() {
	out := os.Stdout

	printOnlyBroker(getBroker(), out)
	printOnlyTrigger(getTrigger(), out)

	writer := tablewriter.NewWriter(out)
	writer.SetHeader([]string{
		"Num Brokers",
		"Num Triggers",
		"Num Objects",
		"Brokers JSON",
		"Brokers Protobuf",
	})
	printBrokersAndTriggers(getBroker(), getTrigger(), 10, 10, writer)
	printBrokersAndTriggers(getBroker(), getTrigger(), 20, 20, writer)
	printBrokersAndTriggers(getBroker(), getTrigger(), 25, 25, writer)
	printBrokersAndTriggers(getBroker(), getTrigger(), 30, 30, writer)
	printBrokersAndTriggers(getBroker(), getTrigger(), 40, 40, writer)
	writer.Render()
}

func getBroker() coreconfig.Broker {
	return coreconfig.Broker{
		Id:             uuid.New().String(),
		Topic:          brokerreconciler.TopicPrefix + strings.Repeat("a", longestBrokerName+longestNamespaceName),
		DeadLetterSink: strings.Repeat("a", longestName+longestNamespaceName) + k8sServiceSuffix, // common case only, it may be longer
		Path:           strings.Repeat("a", longestNamespaceName+longestName+2),
	}

}

func getTrigger() coreconfig.Trigger {
	return coreconfig.Trigger{
		// attributed might be arbitrary long as well as keys and values, so we evaluate it empirically by adding only
		// commonly used filters.
		Attributes: map[string]string{
			"source":  strings.Repeat("a", longestName+longestNamespaceName) + k8sServiceSuffix, // URI-reference
			"type":    strings.Repeat("a", longestName),                                         // SHOULD be prefixed with a reverse-DNS name
			"subject": strings.Repeat("a", longestName),
		},
		Destination: strings.Repeat("a", longestName+longestNamespaceName) + k8sServiceSuffix, // common case only, it may be longer
		Id:          uuid.New().String(),
	}
}

func printBrokersAndTriggers(broker coreconfig.Broker, trigger coreconfig.Trigger, numBrokers, numTriggers int, writer *tablewriter.Table) {

	brokers := getConfigMapData(broker, trigger, numBrokers, numTriggers)

	inJson, inProto := serializeJSONProto(&brokers)
	writer.Append([]string{
		toString(numBrokers),
		toString(numTriggers),
		toString(numBrokers * numTriggers),
		toStringInMB(len(inJson)),
		toStringInMB(len(inProto)),
	})
}

func getConfigMapData(broker coreconfig.Broker, trigger coreconfig.Trigger, numBrokers int, numTriggers int) coreconfig.Brokers {

	brokers := coreconfig.Brokers{
		VolumeGeneration: math.MaxUint64,
	}

	triggers := make([]*coreconfig.Trigger, numTriggers)
	for j := 0; j < numTriggers; j++ {
		triggers = append(triggers, &trigger)
	}
	broker.Triggers = triggers

	for i := 0; i < numBrokers; i++ {
		brokers.Brokers = append(brokers.Brokers, &broker)
	}
	return brokers
}

func printOnlyTrigger(trigger coreconfig.Trigger, out io.Writer) {
	inJson, inProto := serializeJSONProto(&trigger)
	writer := tablewriter.NewWriter(out)
	writer.SetHeader([]string{"Trigger JSON", "Trigger Protobuf"})
	writer.Append([]string{
		toStringInMB(len(inJson)),
		toStringInMB(len(inProto)),
	})
	writer.Render()
}

func printOnlyBroker(broker coreconfig.Broker, out io.Writer) {
	inJson, inProto := serializeJSONProto(&broker)
	writer := tablewriter.NewWriter(out)
	writer.SetHeader([]string{"Broker JSON (No Trigger)", "Broker Protobuf (No Trigger)"})
	writer.Append([]string{
		toStringInMB(len(inJson)),
		toStringInMB(len(inProto)),
	})
	writer.Render()
}

func serializeJSONProto(obj proto.Message) ([]byte, []byte) {
	j, _ := json.Marshal(obj)
	p, _ := proto.Marshal(obj)

	return j, p
}

func toString(n int) string {
	return fmt.Sprintf("%d", n)
}

func toStringInMB(n int) string {
	return fmt.Sprintf("%f", float64(n)/1048576)
	// return toString(n)
}
