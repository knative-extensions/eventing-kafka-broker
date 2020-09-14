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

package addressable

import (
	"context"
	"fmt"
	"sync"
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	testlib "knative.dev/eventing/test/lib"
	pkgtest "knative.dev/pkg/test"
)

func Send(t *testing.T, addressable Addressable) []string {
	return SendN(t, 10, addressable)
}

func SendN(t *testing.T, n int, addressable Addressable) []string {

	idsChan := make(chan string, n)
	ctx := context.Background()

	// Send n messages to the addressable
	go func() {
		var wg sync.WaitGroup
		wg.Add(n)

		for i := 0; i < n; i++ {

			go func(i int) {

				// Client isn't thread safe so we need to create one per Goroutine.
				client, err := testlib.NewClient(
					pkgtest.Flags.Kubeconfig,
					pkgtest.Flags.Cluster,
					addressable.Namespace,
					t,
				)
				assert.Nil(t, err)

				event := cetest.FullEvent()
				id := uuid.New().String()
				event.SetID(id)

				client.Namespace = addressable.Namespace

				client.SendEventToAddressable(ctx, fmt.Sprintf("%s-%d", addressable.Name, i), addressable.Name, &addressable.TypeMeta, event)

				idsChan <- id
				wg.Done()
			}(i)

		}

		wg.Wait()
		close(idsChan)
	}()

	ids := make([]string, 0, n)
	for id := range idsChan {
		ids = append(ids, id)
	}

	return ids
}
