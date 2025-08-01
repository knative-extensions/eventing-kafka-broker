/*
Copyright 2018 The Knative Authors

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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"
	"knative.dev/eventing/pkg/eventingtls"
	"knative.dev/eventing/pkg/observability"
	eventingotel "knative.dev/eventing/pkg/observability/otel"
	"knative.dev/eventing/pkg/observability/resource"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/observability/metrics"
	"knative.dev/pkg/observability/tracing"
	"knative.dev/pkg/signals"

	"github.com/cloudevents/sdk-go/observability/opentelemetry/v2/client"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/kelseyhightower/envconfig"
)

type Heartbeat struct {
	Sequence int    `json:"id"`
	Label    string `json:"label"`
	Msg      string `json:"msg,omitempty"`
}

var (
	eventSource string
	eventType   string
	sink        string
	cacerts     string
	label       string
	periodStr   string
	msg         string
)

func init() {
	flag.StringVar(&eventSource, "eventSource", "", "the event-source (CloudEvents)")
	flag.StringVar(&eventType, "eventType", "dev.knative.eventing.samples.heartbeat", "the event-type (CloudEvents)")
	flag.StringVar(&sink, "sink", "", "the host url to heartbeat to")
	flag.StringVar(&cacerts, "cacerts", "", "the ca cert for the host url to heartbeat to")
	flag.StringVar(&label, "label", "", "a special label")
	flag.StringVar(&periodStr, "period", "5s", "the duration between heartbeats. Supported formats: Go (https://pkg.go.dev/time#ParseDuration), integers (interpreted as seconds)")
	flag.StringVar(&msg, "msg", "", "message content in data.msg")
}

type envConfig struct {
	// Sink URL where to send heartbeat cloudevents
	Sink string `envconfig:"K_SINK"`

	// CACert is the certificate for enabling HTTPS in Sink URL
	CACerts string `envconfig:"K_CA_CERTS"`

	// CEOverrides are the CloudEvents overrides to be applied to the outbound event.
	CEOverrides string `envconfig:"K_CE_OVERRIDES"`

	// Name of this pod.
	Name string `envconfig:"POD_NAME" required:"true"`

	// Namespace this pod exists in.
	Namespace string `envconfig:"POD_NAMESPACE" required:"true"`

	// Whether to run continuously or exit.
	OneShot bool `envconfig:"ONE_SHOT" default:"false"`

	// JSON configuration for tracing
	ObservabilityConfig string `envconfig:"K_OBSERVABILITY_CONFIG"`
}

func main() {
	flag.Parse()

	ctx := signals.NewContext()
	ctx = cloudevents.ContextWithRetriesExponentialBackoff(ctx, 20*time.Millisecond, 10)

	defer maybeQuitIstioProxy()

	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		log.Printf("[ERROR] Failed to process env var: %s", err)
		os.Exit(1)
	}

	if env.Sink != "" {
		sink = env.Sink
	}

	if env.CACerts != "" {
		cacerts = env.CACerts
	}

	var ceOverrides *duckv1.CloudEventOverrides
	if len(env.CEOverrides) > 0 {
		overrides := duckv1.CloudEventOverrides{}
		err := json.Unmarshal([]byte(env.CEOverrides), &overrides)
		if err != nil {
			log.Printf("[ERROR] Unparseable CloudEvents overrides %s: %v", env.CEOverrides, err)
			os.Exit(1)
		}
		ceOverrides = &overrides
	}

	oidcToken, err := os.ReadFile("/oidc/token")
	if err != nil {
		log.Printf("Failed to read OIDC token, client will not send Authorization header: %v", err)
	}

	cfg := &observability.Config{}

	err = json.Unmarshal([]byte(env.ObservabilityConfig), cfg)
	if err != nil {
		log.Printf("failed to parse observability config from env, falling back to defaults (noop)\n")
	}

	cfg = observability.MergeWithDefaults(cfg)

	ctx = observability.WithConfig(ctx, cfg)

	otelResource, err := resource.Default("hearbeat")
	if err != nil {
		log.Printf("failed to correctly initialize otel resource, resouce may be missing some attributes: %s\n", err.Error())
	}

	meterProvider, err := metrics.NewMeterProvider(
		ctx,
		cfg.Metrics,
		metric.WithResource(otelResource),
	)
	if err != nil {
		log.Printf("failed to setup meter provider, falling back to noop: %s\n", err.Error())
		meterProvider = eventingotel.DefaultMeterProvider(ctx, otelResource)
	}

	otel.SetMeterProvider(meterProvider)

	tracerProvider, err := tracing.NewTracerProvider(
		ctx,
		cfg.Tracing,
		trace.WithResource(otelResource),
	)
	if err != nil {
		log.Printf("failed to setup tracing provider, falling back to noop: %s\n", err.Error())
		tracerProvider = eventingotel.DefaultTraceProvider(ctx, otelResource)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()

		if err := meterProvider.Shutdown(ctx); err != nil {
			log.Printf("failed to shut down metrics: %s\n", err.Error())
		}

		if err := tracerProvider.Shutdown(ctx); err != nil {
			log.Printf("failed to shut down tracing: %s\n", err.Error())
		}
	}()

	otel.SetTextMapPropagator(tracing.DefaultTextMapPropagator())
	otel.SetTracerProvider(tracerProvider)

	opts := make([]cehttp.Option, 0, 1)
	opts = append(opts, cloudevents.WithTarget(sink))

	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	if eventingtls.IsHttpsSink(sink) {
		clientConfig := eventingtls.NewDefaultClientConfig()
		clientConfig.CACerts = &cacerts

		baseTransport.TLSClientConfig, err = eventingtls.GetTLSClientConfig(clientConfig)
		if err != nil {
			log.Fatalf("Failed to get TLS Client Config: %v", err)
		}

	}

	transport := otelhttp.NewTransport(
		baseTransport,
		otelhttp.WithPropagators(tracing.DefaultTextMapPropagator()),
	)

	opts = append(opts, cehttp.WithRoundTripper(transport))
	c, err := client.NewClientHTTP(opts, nil)
	if err != nil {
		log.Fatalf("failed to create client: %s", err.Error())
	}

	// default to 5s if unset, try to parse as a duration, then as an int
	var period time.Duration
	if periodStr == "" {
		period = 5 * time.Second
	} else if p, err := time.ParseDuration(periodStr); err == nil {
		period = p
	} else if p, err := strconv.Atoi(periodStr); err == nil {
		period = time.Duration(p) * time.Second
	} else {
		log.Fatalf("Invalid period interval provided: %q", periodStr)
	}

	if eventSource == "" {
		eventSource = fmt.Sprintf("https://knative.dev/eventing-contrib/cmd/heartbeats/#%s/%s", env.Namespace, env.Name)
		log.Printf("Heartbeats Source: %s", eventSource)
	}

	if len(label) > 0 && label[0] == '"' {
		label, _ = strconv.Unquote(label)
	}
	hb := &Heartbeat{
		Sequence: 0,
		Label:    label,
		Msg:      msg,
	}
	ticker := time.NewTicker(period)
	for {
		hb.Sequence++

		event := cloudevents.NewEvent("1.0")
		event.SetType(eventType)
		event.SetSource(eventSource)
		event.SetExtension("the", 42)
		event.SetExtension("heart", "yes")
		event.SetExtension("beats", true)

		if ceOverrides != nil && ceOverrides.Extensions != nil {
			for n, v := range ceOverrides.Extensions {
				event.SetExtension(n, v)
			}
		}

		if err := event.SetData(cloudevents.ApplicationJSON, hb); err != nil {
			log.Printf("failed to set cloudevents msg: %s", err.Error())
		}

		if oidcToken != nil {
			ctx = withAuthHeader(ctx, oidcToken)
		}

		log.Printf("sending cloudevent to %s", sink)
		if res := c.Send(ctx, event); !cloudevents.IsACK(res) {
			log.Printf("failed to send cloudevent: %v", res)
		}

		if env.OneShot {
			return
		}

		// Wait for next tick
		<-ticker.C
	}
}

// maybeQuitIstioProxy shuts down Istio's proxy when available.
func maybeQuitIstioProxy() {
	req, _ := http.NewRequest(http.MethodPost, "http://localhost:15020/quitquitquit", nil)

	_, err := http.DefaultClient.Do(req)

	if err != nil && !errors.Is(err, syscall.ECONNREFUSED) {
		log.Println("[Ignore this warning if Istio proxy is not used on this pod]", err)
	}
}

func withAuthHeader(ctx context.Context, oidcToken []byte) context.Context {
	// Appending the auth token to the outgoing request
	headers := cehttp.HeaderFrom(ctx)
	headers.Set("Authorization", fmt.Sprintf("Bearer %s", oidcToken))
	return cehttp.WithCustomHeader(ctx, headers)
}
