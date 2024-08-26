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
	"fmt"
	"time"

	"knative.dev/eventing/test/rekt/resources/addressable"

	"k8s.io/apimachinery/pkg/runtime/schema"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/manifest"
)

//go:embed *.yaml
var yaml embed.FS

func GVR() schema.GroupVersionResource {
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
func Install(name, topic string, bootstrapServers []string, opts ...manifest.CfgFn) feature.StepFn {
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
	return k8s.IsReady(GVR(), name, timing...)
}

func AsKReference(name string, namespace string) *duckv1.KReference {
	return &duckv1.KReference{
		Kind:       "KafkaSink",
		APIVersion: fmt.Sprintf("%s/%s", GVR().Group, GVR().Version),
		Namespace:  namespace,
		Name:       name,
	}
}

// Address returns a sink's address.
func Address(ctx context.Context, name string, timings ...time.Duration) (*duckv1.Addressable, error) {
	return addressable.Address(ctx, GVR(), name, timings...)
}

// ValidateAddress validates the address retured by Address
func ValidateAddress(name string, validate addressable.ValidateAddressFn, timings ...time.Duration) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		addr, err := Address(ctx, name, timings...)
		if err != nil {
			t.Error(err)
			return
		}
		if err := validate(addr); err != nil {
			t.Error(err)
			return
		}
	}
}

// GoesReady returns a feature that will create a KafkaSink of the given
// name and topic, and confirm it becomes ready.
func GoesReady(name, topic string, bootstrapServers []string, cfg ...manifest.CfgFn) *feature.Feature {
	f := new(feature.Feature)

	f.Setup(fmt.Sprintf("install KafkaSink %q", name), Install(name, topic, bootstrapServers, cfg...))
	f.Setup("KafkaSink is ready", IsReady(name))

	return f
}
