module knative.dev/eventing-kafka-broker

go 1.16

require (
	github.com/Shopify/sarama v1.30.1
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.4.1
	github.com/cloudevents/sdk-go/v2 v2.7.0
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.3.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pierdipi/sacura v0.0.0-20210302185533-982357fc042b
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.7.0
	github.com/xdg-go/scram v1.0.2
	go.uber.org/atomic v1.9.0
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.19.1
	google.golang.org/protobuf v1.27.1
	k8s.io/api v0.21.4
	k8s.io/apiextensions-apiserver v0.21.4
	k8s.io/apimachinery v0.21.4
	k8s.io/apiserver v0.21.4
	k8s.io/client-go v0.21.4
	k8s.io/utils v0.0.0-20210111153108-fddb29f9d009
	knative.dev/eventing v0.28.1-0.20211230085924-1ba7810fba5d
	knative.dev/eventing-kafka v0.28.1-0.20220104114830-15a64fb7261f
	knative.dev/hack v0.0.0-20211222071919-abd085fc43de
	knative.dev/pkg v0.0.0-20211216142117-79271798f696
	knative.dev/reconciler-test v0.0.0-20211222120418-816f2192fec9
)
