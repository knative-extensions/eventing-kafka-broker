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

package kafkachannel

import (
	"context"
	"embed"
	"time"

	"knative.dev/eventing/test/rekt/resources/addressable"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/manifest"
)

//go:embed *.yaml
var yaml embed.FS

func GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "messaging.knative.dev", Version: "v1beta1", Resource: "kafkachannels"}
}

func GVK() schema.GroupVersionKind {
	return schema.ParseGroupKind(EnvCfg.ChannelGK).WithVersion(EnvCfg.ChannelV)
}

var EnvCfg EnvConfig

type EnvConfig struct {
	ChannelGK string `envconfig:"CHANNEL_GROUP_KIND" default:"KafkaChannel.messaging.knative.dev" required:"true"`
	ChannelV  string `envconfig:"CHANNEL_VERSION" default:"v1beta1" required:"true"`
}

// Install will create a KafkaChannel resource, using the latest version, augmented with the config fn options.
func Install(name string, opts ...manifest.CfgFn) feature.StepFn {
	cfg := map[string]interface{}{
		"name":    name,
		"version": GVR().Version,
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

// IsReady tests to see if a KafkaChannel becomes ready within the time given.
func IsReady(name string, timings ...time.Duration) feature.StepFn {
	return k8s.IsReady(GVR(), name, timings...)
}

// WithVersion overrides the default API version
func WithVersion(version string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if version != "" {
			cfg["version"] = version
		}
	}
}

// WithNumPartitions adds the numPartitions config to a KafkaChannel spec.
func WithNumPartitions(numPartitions string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["numPartitions"] = numPartitions
	}
}

// WithReplicationFactor adds the replicationFactor config to a KafkaChannel spec.
func WithReplicationFactor(replicationFactor string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["replicationFactor"] = replicationFactor
	}
}

// WithRetentionDuration adds the retentionDuration config to a KafkaChannel spec.
func WithRetentionDuration(retentionDuration string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["retentionDuration"] = retentionDuration
	}
}

// AsRef returns a KRef for a Channel without namespace.
func AsRef(name string) *duckv1.KReference {
	return &duckv1.KReference{
		Kind:       EnvCfg.ChannelGK,
		APIVersion: EnvCfg.ChannelV,
		Name:       name,
	}
}

// AsRef returns a KRef for a Channel without namespace.
func AsDestinationRef(name string) *duckv1.Destination {
	return &duckv1.Destination{
		Ref: &duckv1.KReference{
			Kind:       EnvCfg.ChannelGK,
			APIVersion: EnvCfg.ChannelV,
			Name:       name,
		},
	}
}

// Address returns a Channel's address.
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
