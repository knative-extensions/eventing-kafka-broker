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
	"log"
	"time"

	"github.com/kelseyhightower/envconfig"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	eventingv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/manifest"
)

// TODO copy pasted from eventing, remove once upstream is clear how templating should work...

var EnvCfg EnvConfig

type EnvConfig struct {
	BrokerClass string `envconfig:"BROKER_CLASS" default:"MTChannelBasedBroker" required:"true"`
}

func init() {
	// Process EventingGlobal.
	if err := envconfig.Process("", &EnvCfg); err != nil {
		log.Fatal("Failed to process env var", err)
	}
}

func Gvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "eventing.knative.dev", Version: "v1", Resource: "brokers"}
}

// WithBrokerClass adds the broker class config to a Broker spec.
func WithBrokerClass(class string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["brokerClass"] = class
	}
}

// WithConfig adds the broker class config to a Broker spec.
func WithConfig(name string) manifest.CfgFn {
	return func(templateData map[string]interface{}) {
		cfg := make(map[string]interface{})
		cfg["kind"] = "ConfigMap"
		cfg["apiVersion"] = "v1"
		cfg["name"] = name
		templateData["config"] = cfg
	}
}

// WithDeadLetterSink adds the dead letter sink related config to a Broker spec.
func WithDeadLetterSink(ref *duckv1.KReference, uri string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, set := cfg["delivery"]; !set {
			cfg["delivery"] = map[string]interface{}{}
		}
		delivery := cfg["delivery"].(map[string]interface{})
		if _, set := delivery["deadLetterSink"]; !set {
			delivery["deadLetterSink"] = map[string]interface{}{}
		}
		dls := delivery["deadLetterSink"].(map[string]interface{})
		if uri != "" {
			dls["uri"] = uri
		}
		if ref != nil {
			if _, set := dls["ref"]; !set {
				dls["ref"] = map[string]interface{}{}
			}
			dref := dls["ref"].(map[string]interface{})
			dref["apiVersion"] = ref.APIVersion
			dref["kind"] = ref.Kind
			// Skip namespace.
			dref["name"] = ref.Name
		}
	}
}

// WithRetry adds the retry related config to a Broker spec.
func WithRetry(count int32, backoffPolicy *eventingv1.BackoffPolicyType, backoffDelay *string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if _, set := cfg["delivery"]; !set {
			cfg["delivery"] = map[string]interface{}{}
		}
		delivery := cfg["delivery"].(map[string]interface{})

		delivery["retry"] = count
		if backoffPolicy != nil {
			delivery["backoffPolicy"] = backoffPolicy
		}
		if backoffDelay != nil {
			delivery["backoffDelay"] = backoffDelay
		}
	}
}

// Install will create a Broker resource, augmented with the config fn options.
func Install(name string, opts ...manifest.CfgFn) feature.StepFn {
	cfg := map[string]interface{}{
		"name": name,
	}
	for _, fn := range opts {
		fn(cfg)
	}

	return func(ctx context.Context, t feature.T) {
		if _, err := manifest.InstallLocalYaml(ctx, cfg); err != nil {
			t.Fatal(err)
		}
	}
}

// IsReady tests to see if a Broker becomes ready within the time given.
func IsReady(name string, timing ...time.Duration) feature.StepFn {
	return k8s.IsReady(Gvr(), name, timing...)
}

// IsAddressable tests to see if a Broker becomes addressable within the  time
// given.
func IsAddressable(name string, timings ...time.Duration) feature.StepFn {
	return k8s.IsAddressable(Gvr(), name, timings...)
}

// Address returns a broker's address.
func Address(ctx context.Context, name string, timings ...time.Duration) (*apis.URL, error) {
	interval, timeout := k8s.PollTimings(ctx, timings)
	var addr *apis.URL
	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		var err error
		addr, err = k8s.Address(ctx, Gvr(), name)
		if err == nil && addr == nil {
			// keep polling
			return false, nil
		}
		if err != nil {
			if apierrors.IsNotFound(err) {
				// keep polling
				return false, nil
			}
			// seems fatal.
			return false, err
		}
		// success!
		return true, nil
	})
	return addr, err
}
