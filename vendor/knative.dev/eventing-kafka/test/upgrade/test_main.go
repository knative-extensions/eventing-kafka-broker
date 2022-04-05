/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package upgrade

import (
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/test"
	eventingTest "knative.dev/eventing/test"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/pkg/system"
)

var (
	channelTestRunner  testlib.ComponentsTestRunner
	defaultChannelType = metav1.TypeMeta{
		APIVersion: "messaging.knative.dev/v1beta1",
		Kind:       "KafkaChannel",
	}
)

// RunMainTest is a module TestMain.
func RunMainTest(m *testing.M) {
	// setting a default channel
	testlib.DefaultChannel = defaultChannelType
	eventingTest.InitializeEventingFlags()
	channelTestRunner = testlib.ComponentsTestRunner{
		ComponentFeatureMap: test.ChannelFeatureMap,
		ComponentsToTest:    eventingTest.EventingFlags.Channels,
	}
	os.Exit(func() int {
		defer testlib.ExportLogs(testlib.SystemLogsDir, system.Namespace())

		return m.Run()
	}())
}
