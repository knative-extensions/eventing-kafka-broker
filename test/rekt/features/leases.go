/*
 * Copyright 2022 The Knative Authors
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

package features

import (
	"context"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/feature"
)

func KafkaChannelLease() *feature.Feature {
	f := feature.NewFeatureNamed("KafkaChannel lease")

	f.Assert("verify lease name", verifyLeaseAcquired(
		"kafkachannel-controller.knative.dev.eventing-kafka.pkg.channel.consolidated.reconciler.controller.reconciler.00-of-01",
	))

	return f
}

func KafkaSourceLease() *feature.Feature {
	f := feature.NewFeatureNamed("KafkaSource lease")

	f.Assert("verify lease name", verifyLeaseAcquired(
		"kafka-controller.knative.dev.eventing-kafka.pkg.source.reconciler.source.reconciler.00-of-01",
	))

	return f
}

func verifyLeaseAcquired(name string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		err := wait.Poll(time.Second, time.Minute, func() (done bool, err error) {
			lease, err := kubeclient.Get(ctx).
				CoordinationV1().
				Leases(system.Namespace()).
				Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal("failed to get lease %s/%s: %v", system.Namespace(), name, err)
			}

			return lease.Spec.HolderIdentity != nil &&
				strings.HasPrefix(*lease.Spec.HolderIdentity, "kafka-controller"), nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
