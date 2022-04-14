//go:build upgrade
// +build upgrade

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

package upgrade

import (
	"log"
	"testing"

	pkgupgrade "knative.dev/pkg/test/upgrade"
	"knative.dev/pkg/test/zipkin"
)

func TestUpgrades(t *testing.T) {
	suite := Suite()
	// Any tests may SetupZipkinTracing, it will only actually be done once. This should be the ONLY
	// place that cleans it up. If an individual test calls this instead, then it will break other
	// tests that need the tracing in place.
	defer zipkin.CleanupZipkinTracingSetup(log.Printf)
	suite.Execute(pkgupgrade.Configuration{T: t})
}
