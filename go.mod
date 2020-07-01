module knative.dev/eventing-kafka-broker

go 1.14

require (
	github.com/Shopify/sarama v1.26.4
	github.com/cloudevents/sdk-go/v2 v2.0.1-0.20200630063327-b91da81265fe
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.2
	github.com/google/go-cmp v0.5.0
	github.com/google/uuid v1.1.1
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/stretchr/testify v1.5.1
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.14.1
	k8s.io/api v0.17.6
	k8s.io/apiextensions-apiserver v0.17.6
	k8s.io/apimachinery v0.18.6
	k8s.io/client-go v11.0.1-0.20190805182717-6502b5e7b1b5+incompatible
	knative.dev/eventing v0.16.1-0.20200721123435-4f98fa7bae59
	knative.dev/pkg v0.0.0-20200719221534-601c9ff3832e
	knative.dev/test-infra v0.0.0-20200721175154-c98db9bd4d5d
)

replace (
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v0.9.2
	k8s.io/api => k8s.io/api v0.17.6
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.17.6
	k8s.io/apimachinery => k8s.io/apimachinery v0.17.6
	k8s.io/client-go => k8s.io/client-go v0.17.6
	k8s.io/code-generator => k8s.io/code-generator v0.17.6
)
