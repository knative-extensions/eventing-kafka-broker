module knative.dev/eventing-kafka-broker

go 1.14

require (
	github.com/Shopify/sarama v1.26.4
	github.com/cloudevents/sdk-go/v2 v2.2.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.2
	github.com/google/go-cmp v0.5.1
	github.com/google/uuid v1.1.1
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/stretchr/testify v1.5.1
	go.uber.org/zap v1.15.0
	k8s.io/api v0.18.7-rc.0
	k8s.io/apiextensions-apiserver v0.18.4
	k8s.io/apimachinery v0.18.7-rc.0
	k8s.io/client-go v11.0.1-0.20190805182717-6502b5e7b1b5+incompatible
	k8s.io/kubernetes v1.14.7
	k8s.io/utils v0.0.0-20200603063816-c1c6865ac451
	knative.dev/eventing v0.16.1-0.20200814220206-e8307cbbcecb
	knative.dev/pkg v0.0.0-20200812224206-44c860147a87
	knative.dev/test-infra v0.0.0-20200813220834-388e55a496cf
)

replace (
	k8s.io/api => k8s.io/api v0.17.6
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.17.6
	k8s.io/apimachinery => k8s.io/apimachinery v0.17.6
	k8s.io/client-go => k8s.io/client-go v0.17.6
	k8s.io/code-generator => k8s.io/code-generator v0.17.6
)
