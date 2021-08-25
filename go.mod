module knative.dev/eventing-kafka-broker

go 1.16

require (
	github.com/Shopify/sarama v1.29.1
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.4.1
	github.com/cloudevents/sdk-go/v2 v2.4.1
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.3.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pierdipi/sacura v0.0.0-20210302185533-982357fc042b
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.7.0
	github.com/xdg/scram v1.0.3
	go.uber.org/zap v1.18.1
	google.golang.org/protobuf v1.27.1
	k8s.io/api v0.20.7
	k8s.io/apiextensions-apiserver v0.20.7
	k8s.io/apimachinery v0.20.7
	k8s.io/apiserver v0.20.7
	k8s.io/client-go v0.20.7
	k8s.io/utils v0.0.0-20210111153108-fddb29f9d009
	knative.dev/eventing v0.25.1-0.20210821205434-128a666cce4b
	knative.dev/eventing-kafka v0.25.0
	knative.dev/hack v0.0.0-20210806075220-815cd312d65c
	knative.dev/pkg v0.0.0-20210822203034-3bed4dcf179c
	knative.dev/reconciler-test v0.0.0-20210820180205-a25de6a08087
)
