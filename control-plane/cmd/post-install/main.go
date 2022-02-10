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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"k8s.io/client-go/kubernetes"
	kcs "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	"knative.dev/pkg/environment"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/signals"
)

func main() {
	ctx := signals.NewContext()

	config, err := logging.NewConfigFromMap(nil)
	if err != nil {
		log.Fatal("Failed to create logging config: ", err)
	}

	logger, _ := logging.NewLoggerFromConfig(config, "kafka-broker-post-install")
	defer logger.Sync()

	logging.WithLogger(ctx, logger)

	if err := run(ctx); err != nil {
		logger.Fatal(err)
	}
}

func run(ctx context.Context) error {

	env := environment.ClientConfig{}
	env.InitFlags(flag.CommandLine)
	flag.Parse()

	config, err := env.GetRESTConfig()
	if err != nil {
		return fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	migrator := &KafkaSourceMigrator{
		kcs: kcs.NewForConfigOrDie(config),
		k8s: kubernetes.NewForConfigOrDie(config),
	}

	return migrator.Migrate(ctx)
}
