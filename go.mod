module knative.dev/eventing-kafka-broker

go 1.15

require (
	github.com/Shopify/sarama v1.28.0
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.2.0
	github.com/cloudevents/sdk-go/v2 v2.4.1
	github.com/google/go-cmp v0.5.5
	github.com/google/uuid v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pierdipi/sacura v0.0.0-20210302185533-982357fc042b
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.7.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.uber.org/zap v1.16.0
	google.golang.org/protobuf v1.26.0
	k8s.io/api v0.19.7
	k8s.io/apiextensions-apiserver v0.19.7
	k8s.io/apimachinery v0.19.7
	k8s.io/apiserver v0.19.7
	k8s.io/client-go v0.19.7
	k8s.io/utils v0.0.0-20210111153108-fddb29f9d009
	knative.dev/eventing v0.22.1-0.20210423044837-a0a33025aee0
	knative.dev/hack v0.0.0-20210325223819-b6ab329907d3
	knative.dev/pkg v0.0.0-20210422210038-0c5259d6504d
	knative.dev/reconciler-test v0.0.0-20210423124638-9a9c4dd3af2b
)
