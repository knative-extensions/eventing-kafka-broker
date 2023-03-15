//go:build e2e
// +build e2e

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

package conformance

import (
	"log"
	"os"
	"testing"

	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	_ "knative.dev/pkg/system/testing"
	"knative.dev/pkg/test/zipkin"

	"knative.dev/reconciler-test/pkg/environment"
)

var global environment.GlobalEnvironment

// TestMain in the conformance package allows running the tests
// against any Broker class.
func TestMain(m *testing.M) {
	global = environment.NewStandardGlobalEnvironment()
	os.Exit(func() int {
		defer zipkin.CleanupZipkinTracingSetup(log.Printf)
		return m.Run()
	}())
}
