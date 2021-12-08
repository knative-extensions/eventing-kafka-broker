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

package upgrade_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"knative.dev/eventing-kafka-broker/test/upgrade"
)

func TestSuite(t *testing.T) {
	s := upgrade.Suite()
	assert.NotEmpty(t, s.Tests.Continual)
	assert.NotEmpty(t, s.Tests.PostDowngrade)
	assert.NotEmpty(t, s.Tests.PreUpgrade)
	assert.NotEmpty(t, s.Tests.PostUpgrade)

	assert.NotEmpty(t, s.Installations.Base)
	assert.NotEmpty(t, s.Installations.UpgradeWith)
	assert.NotEmpty(t, s.Installations.DowngradeWith)
}
