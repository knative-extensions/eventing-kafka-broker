/*
 * Copyright 2024 The Knative Authors
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

package installation

import (
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
	pkgupgrade "knative.dev/pkg/test/upgrade"
	"knative.dev/reconciler-test/pkg/environment"
)

func cleanupChannelv2ConsumerGroups(c pkgupgrade.Context, glob environment.GlobalEnvironment) {
	ctx, _ := glob.Environment()
	client := kubeclient.Get(ctx)

	err := deleteConsumerGroups(ctx, client, "Channel")
	if err != nil {
		c.T.Fatal("failed to downgrade from triggerv2", err.Error())
	}
}

func cleanupChannelv2Deployments(c pkgupgrade.Context, glob environment.GlobalEnvironment) {
	ctx, _ := glob.Environment()
	client := kubeclient.Get(ctx)
	err := deleteStatefulSet(ctx, client, "kafka-channel-receiver", system.Namespace())
	if err != nil {
		c.T.Fatal("failed to downgrade from channelv2", err.Error())
	}

	deleteStatefulSet(ctx, client, "kafka-channel-dispatcher", system.Namespace())
	if err != nil {
		c.T.Fatal("failed to downgrade from channelv2", err.Error())
	}
}
