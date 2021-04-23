// +build e2e

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

package e2e_new

import (
	"testing"

	"knative.dev/control-protocol/test/conformance/feature"
	"knative.dev/pkg/test"
)

func TestControlProtocolConformance(t *testing.T) {
	ctx, env := global.Environment()

	// Test that a Broker can act as middleware.
	env.Test(ctx, t, feature.ConformanceFeature(
		test.ImagePath("conformance-go-client"),
		test.ImagePath("control-protocol-conformance-java-server"),
	))

	env.Finish()
}
