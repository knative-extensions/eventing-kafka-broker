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

package kafkasource

import (
	"context"
	"embed"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/manifest"
)

//go:embed *.yaml
var yaml embed.FS

func GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "sources.knative.dev", Version: "v1beta1", Resource: "kafkasources"}
}

// Install will create a KafkaSource resource, using the latest version, augmented with the config fn options.
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
			t.Fatal(err, cfg)
		}
	}
}

// IsReady tests to see if a KafkaSource becomes ready within the time given.
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

// WithAnnotations adds annotation to a KafkaSource metadata.
func WithAnnotations(annotations map[string]string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if annotations != nil {
			cfg["annotations"] = annotations
		}
	}
}

// WithBootstrapServers adds the bootstrapServers config to a KafkaSource spec.
func WithBootstrapServers(bootstrapServers []string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if bootstrapServers != nil {
			cfg["bootstrapServers"] = bootstrapServers
		}
	}
}

// WithTopics adds the topics config to a KafkaSource spec.
func WithTopics(topics []string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if topics != nil {
			cfg["topics"] = topics
		}
	}
}

// WithTLSEnabled enables TLS to a KafkaSource spec.
func WithTLSEnabled() manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, ok := cfg["tls"]; !ok {
			cfg["tls"] = map[string]interface{}{}
		}
	}
}

// WithTLSCert adds the TLS cert config to a KafkaSource spec.
func WithTLSCert(name, key string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, ok := cfg["tls"]; !ok {
			cfg["tls"] = map[string]interface{}{}
		}
		tls := cfg["tls"].(map[string]interface{})
		if _, ok := tls["cert"]; !ok {
			tls["cert"] = map[string]interface{}{}
		}
		cert := tls["cert"].(map[string]interface{})
		cert["name"] = name
		cert["key"] = key
	}
}

// WithTLSKey adds the TLS key config to a KafkaSource spec.
func WithTLSKey(name, key string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, ok := cfg["tls"]; !ok {
			cfg["tls"] = map[string]interface{}{}
		}
		tls := cfg["tls"].(map[string]interface{})
		if _, ok := tls["key"]; !ok {
			tls["key"] = map[string]interface{}{}
		}
		cert := tls["key"].(map[string]interface{})
		cert["name"] = name
		cert["key"] = key
	}
}

// WithTLSCACert adds the TLS caCert config to a KafkaSource spec.
func WithTLSCACert(name, key string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, ok := cfg["tls"]; !ok {
			cfg["tls"] = map[string]interface{}{}
		}
		tls := cfg["tls"].(map[string]interface{})
		if _, ok := tls["caCert"]; !ok {
			tls["caCert"] = map[string]interface{}{}
		}
		cert := tls["caCert"].(map[string]interface{})
		cert["name"] = name
		cert["key"] = key
	}
}

// WithSASLEnabled enables SASL to a KafkaSource spec.
func WithSASLEnabled() manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, ok := cfg["sasl"]; !ok {
			cfg["sasl"] = map[string]interface{}{}
		}
	}
}

// WithSASLUser adds the SASL user config to a KafkaSource spec.
func WithSASLUser(name, key string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, ok := cfg["sasl"]; !ok {
			cfg["sasl"] = map[string]interface{}{}
		}
		sasl := cfg["sasl"].(map[string]interface{})
		if _, ok := sasl["user"]; !ok {
			sasl["user"] = map[string]interface{}{}
		}
		user := sasl["user"].(map[string]interface{})
		user["name"] = name
		user["key"] = key
	}
}

// WithSASLPassword adds the SASL password config to a KafkaSource spec.
func WithSASLPassword(name, key string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, ok := cfg["sasl"]; !ok {
			cfg["sasl"] = map[string]interface{}{}
		}
		sasl := cfg["sasl"].(map[string]interface{})
		if _, ok := sasl["password"]; !ok {
			sasl["password"] = map[string]interface{}{}
		}
		password := sasl["password"].(map[string]interface{})
		password["name"] = name
		password["key"] = key
	}
}

// WithSASLType adds the SASL type config to a KafkaSource spec.
func WithSASLType(name, key string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, ok := cfg["sasl"]; !ok {
			cfg["sasl"] = map[string]interface{}{}
		}
		sasl := cfg["sasl"].(map[string]interface{})
		if _, ok := sasl["type"]; !ok {
			sasl["type"] = map[string]interface{}{}
		}
		t := sasl["type"].(map[string]interface{})
		t["name"] = name
		t["key"] = key
	}
}

// WithSink adds the sink related config to a KafkaSource spec.
func WithSink(ref *duckv1.KReference, uri string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, set := cfg["sink"]; !set {
			cfg["sink"] = map[string]interface{}{}
		}
		sink := cfg["sink"].(map[string]interface{})

		if uri != "" {
			sink["uri"] = uri
		}
		if ref != nil {
			if _, set := sink["ref"]; !set {
				sink["ref"] = map[string]interface{}{}
			}
			sref := sink["ref"].(map[string]interface{})
			sref["apiVersion"] = ref.APIVersion
			sref["kind"] = ref.Kind
			// skip namespace
			sref["name"] = ref.Name
		}
	}
}

// WithExtensions set ceoverrides.extensions to a KafkaSource spec.
func WithExtensions(extensions map[string]string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if extensions != nil {
			cfg["extensions"] = extensions
		}
	}
}
