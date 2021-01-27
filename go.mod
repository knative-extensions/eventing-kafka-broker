module knative.dev/eventing-kafka-broker

go 1.14

require (
	github.com/Shopify/sarama v1.27.0
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.2.0
	github.com/cloudevents/sdk-go/v2 v2.3.1
	github.com/golang/protobuf v1.4.3
	github.com/google/go-cmp v0.5.4
	github.com/google/uuid v1.1.2
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pierdipi/sacura v0.0.0-20201003135834-e90e3a725ff9
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.6.1
	go.uber.org/zap v1.16.0
	google.golang.org/protobuf v1.25.0
	k8s.io/api v0.19.7
	k8s.io/apiextensions-apiserver v0.19.7
	k8s.io/apimachinery v0.19.7
	k8s.io/apiserver v0.19.7
	k8s.io/client-go v0.19.7
	k8s.io/utils v0.0.0-20210111153108-fddb29f9d009
	knative.dev/eventing v0.20.1-0.20210127002130-ce099cf65fec
	knative.dev/hack v0.0.0-20210120165453-8d623a0af457
	knative.dev/pkg v0.0.0-20210125222030-6040b3af4803
)
