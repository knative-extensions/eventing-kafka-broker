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

package source

import (
	"testing"

	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker/fake"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger/fake"
	_ "knative.dev/pkg/client/injection/ducks/duck/v1/addressable/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	_ "knative.dev/eventing-kafka/pkg/client/injection/informers/sources/v1beta1/kafkasource/fake"

	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup/fake"
)

func TestNewControllerV2(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	controller := NewController(ctx)
	if controller == nil {
		t.Error("failed to create controller: <nil>")
	}
}
