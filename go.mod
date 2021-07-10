module knative.dev/eventing-kafka-broker

go 1.16

require (
	github.com/Shopify/sarama v1.28.0
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.2.0
	github.com/cloudevents/sdk-go/v2 v2.4.1
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pierdipi/sacura v0.0.0-20210302185533-982357fc042b
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.7.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.uber.org/zap v1.17.0
	google.golang.org/protobuf v1.26.0
	k8s.io/api v0.20.7
	k8s.io/apiextensions-apiserver v0.20.7
	k8s.io/apimachinery v0.20.7
	k8s.io/apiserver v0.20.7
	k8s.io/client-go v0.20.7
	k8s.io/utils v0.0.0-20210111153108-fddb29f9d009
	knative.dev/eventing v0.24.1-0.20210708130023-221dfdfced62
	knative.dev/hack v0.0.0-20210622141627-e28525d8d260
	knative.dev/pkg v0.0.0-20210706174620-fe90576475ca
	knative.dev/reconciler-test v0.0.0-20210707164418-32f0df0c7399
)
