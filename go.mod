module knative.dev/eventing-kafka-broker

go 1.16

require (
	github.com/Shopify/sarama v1.33.0
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.4.1
	github.com/cloudevents/sdk-go/v2 v2.10.1
	github.com/google/go-cmp v0.5.7
	github.com/google/uuid v1.3.0
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/klauspost/compress v1.15.4 // indirect
	github.com/manifestival/client-go-client v0.5.0
	github.com/manifestival/manifestival v0.7.1
	github.com/openzipkin/zipkin-go v0.3.0
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.7.0
	github.com/xdg-go/scram v1.1.1
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/trace v0.20.0
	go.uber.org/atomic v1.9.0
	go.uber.org/multierr v1.8.0
	go.uber.org/zap v1.21.0
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/net v0.0.0-20220524220425-1d687d428aca // indirect
	gonum.org/v1/gonum v0.0.0-20190331200053-3d26580ed485 // indirect
	google.golang.org/protobuf v1.27.1
	k8s.io/api v0.23.9
	k8s.io/apiextensions-apiserver v0.23.9
	k8s.io/apimachinery v0.23.9
	k8s.io/apiserver v0.23.9
	k8s.io/client-go v0.23.9
	k8s.io/utils v0.0.0-20220210201930-3a6ce19ff2f9
	knative.dev/eventing v0.33.1-0.20220809080920-c884e27795f7
	knative.dev/eventing-kafka v0.33.1-0.20220805134425-be98ac8a581d
	knative.dev/hack v0.0.0-20220728013938-9dabf7cf62e3
	knative.dev/pkg v0.0.0-20220805012121-7b8b06028e4f
	knative.dev/reconciler-test v0.0.0-20220805132123-091fb1ad9e9d
	sigs.k8s.io/yaml v1.3.0
)

replace (
	// FIXME: remove this pin when knative/eventing#6451 is merged
	knative.dev/eventing => github.com/cardil/knative-eventing v0.11.1-0.20220718191420-28d8469b82bf
	// FIXME: remove this pin when knative-sandbox/reconciler-test#294 is merged
	knative.dev/reconciler-test => github.com/cardil/knative-reconciler-test v0.0.0-20220715181934-169264f8ba23
)
