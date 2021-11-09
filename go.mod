module knative.dev/eventing-kafka-broker

go 1.16

require (
	github.com/Shopify/sarama v1.30.0
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.4.1
	github.com/cloudevents/sdk-go/v2 v2.4.1
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.3.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pierdipi/sacura v0.0.0-20210302185533-982357fc042b
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.7.0
	github.com/xdg-go/scram v1.0.2
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.19.1
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/net v0.0.0-20211105192438-b53810dc28af // indirect
	google.golang.org/protobuf v1.27.1
	k8s.io/api v0.21.4
	k8s.io/apiextensions-apiserver v0.21.4
	k8s.io/apimachinery v0.21.4
	k8s.io/apiserver v0.21.4
	k8s.io/client-go v0.21.4
	k8s.io/utils v0.0.0-20210111153108-fddb29f9d009
	knative.dev/eventing v0.27.1-0.20211108221643-a72cfc91c421
	knative.dev/eventing-kafka v0.27.1-0.20211108161601-76680cab3d62
	knative.dev/hack v0.0.0-20211108170701-96aac1c30be3
	knative.dev/pkg v0.0.0-20211108064904-3cc697a3cb09
	knative.dev/reconciler-test v0.0.0-20211101213739-c5f938aa9974
)
