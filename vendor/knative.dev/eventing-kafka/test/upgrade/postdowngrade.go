/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package upgrade

import (
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

// ChannelPostDowngradeTest tests channel basic channel operations after
// downgrade.
func ChannelPostDowngradeTest() pkgupgrade.Operation {
	return pkgupgrade.NewOperation("ChannelPostDowngradeTest",
		func(c pkgupgrade.Context) {
			runChannelSmokeTest(c.T)
		})
}

// SourcePostDowngradeTest tests source operations after downgrade.
func SourcePostDowngradeTest() pkgupgrade.Operation {
	return pkgupgrade.NewOperation("SourcePostDowngradeTest",
		func(c pkgupgrade.Context) {
			runSourceSmokeTest(c.T)
		})
}
