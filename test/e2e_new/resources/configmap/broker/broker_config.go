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

package broker

import (
	"context"
	"embed"

	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
)

//go:embed *.yaml
var yaml embed.FS

func Install(name string, opts ...manifest.CfgFn) feature.StepFn {
	cfg := map[string]interface{}{
		"name": name,
	}
	for _, fn := range opts {
		fn(cfg)
	}
	return func(ctx context.Context, t feature.T) {
		if _, err := manifest.InstallYamlFS(ctx, yaml, cfg); err != nil {
			t.Fatal(err)
		}
	}
}

// WithReplicationFactor adds the replication factor
func WithReplicationFactor(replicationFactor int16) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["replicationFactor"] = replicationFactor
	}
}

// WithNumPartitions adds the num of partitions
func WithNumPartitions(numPartitions int32) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["numPartitions"] = numPartitions
	}
}

// WithBootstrapServer adds the bootstrap server
func WithBootstrapServer(bootstrapServer string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["bootstrapServer"] = bootstrapServer
	}
}
