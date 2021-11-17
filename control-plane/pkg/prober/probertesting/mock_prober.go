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

package probertesting

import (
	"context"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
)

// MockProber returns a prober that always returns the provided status.
func MockProber(status prober.Status) prober.Prober {
	return prober.Func(func(ctx context.Context, addressable prober.Addressable) prober.Status {
		return status
	})
}
