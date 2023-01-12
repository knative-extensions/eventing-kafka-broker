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

package kafkasink

import (
	"context"
	"embed"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/manifest"
)

//go:embed *.yaml
var yaml embed.FS

func gvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "eventing.knative.dev", Version: "v1alpha1", Resource: "kafkasinks"}
}

// WithContentMode adds the content mode
func WithContentMode(contentMode string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["contentMode"] = contentMode
	}
}

// WithNumPartitions adds the num of partitions
func WithNumPartitions(numPartitions int32) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["numPartitions"] = numPartitions
	}
}

// WithReplicationFactor adds the replication factor
func WithReplicationFactor(replicationFactor int16) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["replicationFactor"] = replicationFactor
	}
}

// WithAuthSecretName adds the auth secret name
func WithAuthSecretName(name string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["authSecretName"] = name
	}
}

// Install will create a Trigger resource, augmented with the config fn options.
func Install(name, topic, bootstrapServers []string, opts ...manifest.CfgFn) feature.StepFn {
	cfg := map[string]interface{}{
		"name":             name,
		"topic":            topic,
		"bootstrapServers": bootstrapServers,
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

// IsReady tests to see if a Trigger becomes ready within the time given.
func IsReady(name string, timing ...time.Duration) feature.StepFn {
	return k8s.IsReady(gvr(), name, timing...)
}
