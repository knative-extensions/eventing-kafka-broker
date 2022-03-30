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
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/pkg/system"
	pkgupgrade "knative.dev/pkg/test/upgrade"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/test/e2e"
	eventingkafkaupgrade "knative.dev/eventing-kafka/test/upgrade"
)

// BrokerPostUpgradeTest tests channel operations after upgrade.
func BrokerPostUpgradeTest() pkgupgrade.Operation {
	return pkgupgrade.NewOperation("BrokerPostUpgradeTest", func(c pkgupgrade.Context) {
		c.T.Parallel()
		c.T.Run("Verify post-install", func(t *testing.T) {
			verifyPostInstall(t)
		})
		c.T.Run("tests", func(t *testing.T) {
			runBrokerSmokeTest(t)
		})
	})
}

// ChannelPostUpgradeTest tests channel operations after upgrade.
func ChannelPostUpgradeTest() pkgupgrade.Operation {
	return eventingkafkaupgrade.ChannelPostUpgradeTest()
}

// SinkPostUpgradeTest tests sink basic operations post upgrade.
func SinkPostUpgradeTest() pkgupgrade.Operation {
	return pkgupgrade.NewOperation("SinkPostUpgradeTest", func(c pkgupgrade.Context) {
		c.T.Parallel()
		c.T.Run("Verify post-install", func(t *testing.T) {
			verifyPostInstall(t)
		})
		c.T.Run("tests", func(t *testing.T) {
			e2e.RunTestKafkaSink(t, eventing.ModeBinary, nil)
		})
	})
}

func verifyPostInstall(t *testing.T) {
	t.Parallel()

	const (
		postInstallJob            = "kafka-controller-post-install"
		storageVersionMigratorJob = "knative-kafka-storage-version-migrator"
	)
	for _, name := range []string{postInstallJob, storageVersionMigratorJob} {
		t.Run(name, func(t *testing.T) {
			client := testlib.Setup(t, true)
			defer testlib.TearDown(client)

			var lastJob *batchv1.Job
			err := wait.Poll(5*time.Second, 10*time.Minute, func() (done bool, err error) {
				lastJob, err = client.Kube.
					BatchV1().
					Jobs(system.Namespace()).
					Get(context.Background(), name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}

				if lastJob.Status.Failed == *lastJob.Spec.BackoffLimit {
					return false, fmt.Errorf("job %s failed", name)
				}

				return lastJob.Status.Succeeded > 0, nil
			})
			if err != nil {
				j := []byte("unknown")
				if lastJob != nil {
					j, _ = json.Marshal(lastJob)
				}
				t.Fatal(err, "\njob:\n", string(j))
			}
		})
	}
}
