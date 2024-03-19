//go:build e2e
// +build e2e

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

package e2e_channel

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"

	eventingTest "knative.dev/eventing/test"

	"k8s.io/client-go/kubernetes"
	"knative.dev/eventing-kafka-broker/test/pkg/logging"
	testlib "knative.dev/eventing/test/lib"
	pkgtest "knative.dev/pkg/test"
)

var channelTestRunner testlib.ComponentsTestRunner

func TestMain(m *testing.M) {

	eventingTest.InitializeEventingFlags()
	flag.Parse()

	channelTestRunner = testlib.ComponentsTestRunner{
		ComponentFeatureMap: ChannelFeatureMap,
		ComponentsToTest:    eventingTest.EventingFlags.Channels,
	}

	os.Exit(func() int {
		// make sure that this context only cancels after the tests finish running
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config, err := pkgtest.Flags.GetRESTConfig()
		if err != nil {
			log.Printf("Failed to create REST config: %v\n", err)
		}

		kubeClient, err := kubernetes.NewForConfig(config)
		if err != nil {
			log.Printf("Failed to create kube client: %v\n", err)
		}
		logger := logging.NewLogger(ctx, kubeClient, map[string][]string{"knative-eventing": {"kafka-broker-dispatcher", "kafka-broker-receiver", "kafka-sink-receiver", "kafka-channel-receiver", "kafka-channel-dispatcher", "kafka-source-dispatcher", "kafka-webhook-eventing", "kafka-controller", "kafka-source-controller", "eventing-webhook"}})
		e2e_channel_err := logger.Start()
		if err != nil {
			fmt.Printf("failed to start logger: %s", e2e_channel_err.Error())
		}
		return m.Run()
	}())
}
