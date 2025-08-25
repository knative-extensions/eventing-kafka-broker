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

package conformance

import (
	"flag"
	"os"
	"testing"

	e2echannel "knative.dev/eventing-kafka-broker/test/e2e_channel"
	eventingTest "knative.dev/eventing/test"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/pkg/system"
)

var channelTestRunner testlib.ComponentsTestRunner

func TestMain(m *testing.M) {

	os.Exit(func() int {
		eventingTest.InitializeEventingFlags()
		flag.Parse()

		channelTestRunner = testlib.ComponentsTestRunner{
			ComponentFeatureMap: e2echannel.ChannelFeatureMap,
			ComponentsToTest:    eventingTest.EventingFlags.Channels,
		}

		defer testlib.ExportLogs(testlib.SystemLogsDir, system.Namespace())

		return m.Run()
	}())
}
