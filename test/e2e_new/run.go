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
	"fmt"
	"testing"
)

const (
	// Number of times to run a test function.
	rerunTimes = 5
)

// RunMultiple run test function f `rerunTimes` times.
func RunMultiple(t *testing.T, f func(t *testing.T)) {
	RunMultipleN(t, rerunTimes, f)
}

// RunMultipleN run test function f n times.
func RunMultipleN(t *testing.T, n int, f func(t *testing.T)) {
	t.Parallel()

	if testing.Short() {
		n = 1
	}

	for i := 0; i < n; i++ {
		t.Run(fmt.Sprintf("%d", i), f)
	}
}
