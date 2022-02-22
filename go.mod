module knative.dev/eventing-kafka-broker

go 1.16

require (
	github.com/Shopify/sarama v1.31.1
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.4.1
	github.com/cloudevents/sdk-go/v2 v2.8.0
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.3.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pierdipi/sacura v0.0.0-20210302185533-982357fc042b
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.7.0
	github.com/xdg-go/scram v1.1.0
	go.uber.org/atomic v1.9.0
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.19.1
	google.golang.org/protobuf v1.27.1
	k8s.io/api v0.22.5
	k8s.io/apiextensions-apiserver v0.22.5
	k8s.io/apimachinery v0.22.5
	k8s.io/apiserver v0.22.5
	k8s.io/client-go v0.22.5
	k8s.io/utils v0.0.0-20211208161948-7d6a63dca704
	knative.dev/eventing v0.29.1-0.20220201083031-25bf6fe36dfe
	knative.dev/eventing-kafka v0.28.1-0.20220201082931-c084e4c42ad3
	knative.dev/hack v0.0.0-20220201013531-82bfca153560
	knative.dev/pkg v0.0.0-20220201132031-8681fe2035b9
	knative.dev/reconciler-test v0.0.0-20220126171745-740e77ebaace
)
