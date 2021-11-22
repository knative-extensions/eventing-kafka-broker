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

package prober

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestFuncProbe(t *testing.T) {

	calls := atomic.NewInt32(0)
	status := StatusReady
	p := Func(func(ctx context.Context, addressable Addressable) Status {
		calls.Inc()
		return status
	})

	s := p.Probe(context.Background(), Addressable{})

	require.Equal(t, status, s, s.String())
	require.Equal(t, int32(1), calls.Load())
}
