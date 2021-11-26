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

package continual

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
	"knative.dev/eventing/test/upgrade/prober"
	"knative.dev/eventing/test/upgrade/prober/sut"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

func continualVerification(
	testName string,
	opts *TestOptions,
	defaultSut sut.SystemUnderTest,
	configTemplate string,
) pkgupgrade.BackgroundOperation {
	return prober.NewContinualVerification(
		testName,
		verificationOptions(
			opts,
			resolveSut(testName, opts, defaultSut),
			configTemplate,
		),
	)
}

func resolveSut(
	testname string,
	opts *TestOptions,
	defaultSut sut.SystemUnderTest,
) sut.SystemUnderTest {
	var resolved sut.SystemUnderTest
	if opts.SUTs != nil {
		resolved = opts.SUTs[testname]
	}
	if resolved == nil {
		resolved = defaultSut
	}
	return resolved
}

func verificationOptions(
	opts *TestOptions,
	theSut sut.SystemUnderTest,
	configTemplate string,
) prober.ContinualVerificationOptions {
	return prober.ContinualVerificationOptions{
		Configurators: append(
			opts.Configurators,
			configurator(theSut, configTemplate),
		),
		ClientOptions: opts.ClientOptions,
	}
}

func configurator(theSut sut.SystemUnderTest, configTemplate string) prober.Configurator {
	return func(config *prober.Config) error {
		config.SystemUnderTest = theSut
		// TODO: knative/eventing#5176 - this is cumbersome
		config.ConfigTemplate = fmt.Sprintf("../../../../../../%s",
			configTemplate)
		// envconfig.Process invocation is repeated from within prober.NewConfig to
		// make sure every knob is configurable, but using defaults from Eventing
		// Kafka instead of Core. The prefix is also changed.
		return envconfig.Process("eventing_kafka_upgrade_tests", config)
	}
}
