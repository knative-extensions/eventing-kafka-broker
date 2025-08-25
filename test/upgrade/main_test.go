//go:build upgrade
// +build upgrade

/*
 * Copyright 2023 The Knative Authors
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

package upgrade

import (
	"context"
	"log"
	"os"
	"testing"

	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	pkgTest "knative.dev/pkg/test"

	_ "knative.dev/pkg/system/testing"
	"knative.dev/reconciler-test/pkg/environment"

	"knative.dev/eventing-kafka-broker/test/pkg/logging"
)

var global environment.GlobalEnvironment

func TestMain(m *testing.M) {
	restConfig, err := pkgTest.Flags.ClientConfig.GetRESTConfig()
	if err != nil {
		log.Fatal("Error building client config: ", err)
	}

	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Fatal("failed to create kube client: ", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	logger := logging.NewLogger(ctx, kubeClient, logging.CommonLoggingLabels)

	err = logger.Start()
	if err != nil {
		log.Fatal("failed to start logging: ", err)
	}

	// Getting the rest config explicitly and passing it further will prevent re-initializing the flagset
	// in NewStandardGlobalEnvironment(). The upgrade tests use knative.dev/pkg/test which initializes the
	// flagset as well.
	global = environment.NewStandardGlobalEnvironment(func(cfg environment.Configuration) environment.Configuration {
		cfg.Config = restConfig
		return cfg
	})

	// Run the tests.
	os.Exit(func() int {
		defer cancel()
		return m.Run()
	}())
}
