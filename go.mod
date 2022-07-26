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
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.7.0
	github.com/xdg-go/scram v1.1.1
	go.uber.org/atomic v1.9.0
	go.uber.org/multierr v1.8.0
	go.uber.org/zap v1.21.0
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/net v0.0.0-20220524220425-1d687d428aca // indirect
	gonum.org/v1/gonum v0.0.0-20190331200053-3d26580ed485 // indirect
	google.golang.org/protobuf v1.27.1
	k8s.io/api v0.23.8
	k8s.io/apiextensions-apiserver v0.23.8
	k8s.io/apimachinery v0.23.8
	k8s.io/apiserver v0.23.8
	k8s.io/client-go v0.23.8
	k8s.io/utils v0.0.0-20220210201930-3a6ce19ff2f9
	knative.dev/eventing v0.33.0
	knative.dev/eventing-kafka v0.33.1-0.20220721151820-bbc5949bc8ab
	knative.dev/hack v0.0.0-20220701014203-65c463ac8c98
	knative.dev/pkg v0.0.0-20220705130606-e60d250dc637
	knative.dev/reconciler-test v0.0.0-20220705155206-f05db88effbe
	sigs.k8s.io/yaml v1.3.0
)
