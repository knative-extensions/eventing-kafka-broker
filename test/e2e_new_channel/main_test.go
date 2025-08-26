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

package e2e_new_channel

import (
	"context"
	"fmt"
	"os"
	"testing"

	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"knative.dev/eventing/test/rekt/resources/channel_impl"
	_ "knative.dev/pkg/system/testing"

	kubeclient "knative.dev/pkg/client/injection/kube/client"

	"knative.dev/eventing-kafka-broker/test/pkg/logging"
	"knative.dev/reconciler-test/pkg/environment"
)

// global is the singleton instance of GlobalEnvironment. It is used to parse
// the testing config for the test run. The config will specify the cluster
// config as well as the parsing level and state flags.
var global environment.GlobalEnvironment

// TestMain is the first entry point for `go test`.
func TestMain(m *testing.M) {
	global = environment.NewStandardGlobalEnvironment()

	channel_impl.EnvCfg.ChannelGK = "KafkaChannel.messaging.knative.dev"
	channel_impl.EnvCfg.ChannelV = "v1beta1"

	// Run the tests.
	os.Exit(func() int {
		ctx, _ := global.Environment()
		// make sure that this context only cancels after the tests finish running
		ctx = context.WithoutCancel(ctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		client := kubeclient.Get(ctx)
		logger := logging.NewLogger(ctx, client, logging.CommonLoggingLabels)
		err := logger.Start()
		if err != nil {
			fmt.Printf("failed to start logger: %s", err.Error())
		}
		return m.Run()
	}())
}
