module knative.dev/eventing-kafka-broker

go 1.19

require (
	github.com/Shopify/sarama v1.37.0
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.4.1
	github.com/cloudevents/sdk-go/v2 v2.13.0
	github.com/google/go-cmp v0.5.9
	github.com/google/uuid v1.3.0
	github.com/hashicorp/errwrap v1.1.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/klauspost/compress v1.15.11 // indirect
	github.com/magiconair/properties v1.8.5
	github.com/manifestival/client-go-client v0.5.0
	github.com/manifestival/manifestival v0.7.1
	github.com/openzipkin/zipkin-go v0.3.0
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rickb777/date v1.14.1
	github.com/stretchr/testify v1.8.0
	github.com/xdg-go/scram v1.1.1
	go.opentelemetry.io/otel v1.10.0
	go.opentelemetry.io/otel/trace v1.10.0
	go.uber.org/atomic v1.9.0
	go.uber.org/multierr v1.8.0
	go.uber.org/zap v1.21.0
	google.golang.org/protobuf v1.28.1
	k8s.io/api v0.26.5
	k8s.io/apiextensions-apiserver v0.26.5
	k8s.io/apimachinery v0.26.5
	k8s.io/apiserver v0.26.5
	k8s.io/client-go v0.26.5
	k8s.io/utils v0.0.0-20221108210102-8e77b1f39fe2
	sigs.k8s.io/yaml v1.3.0
)

require (
	github.com/google/gofuzz v1.2.0
	github.com/kedacore/keda/v2 v2.8.1
	knative.dev/eventing v0.37.1-0.20230526130526-2a5a9a5134d4
	knative.dev/hack v0.0.0-20230524013611-5812c57cc1ac
	knative.dev/pkg v0.0.0-20230525143525-9bda38b21643
	knative.dev/reconciler-test v0.0.0-20230525131026-c4a6e7c5a8e5
	sigs.k8s.io/controller-runtime v0.12.3
)

require (
	cloud.google.com/go/compute v1.9.0 // indirect
	cloud.google.com/go/iam v0.3.0 // indirect
	github.com/emicklei/go-restful/v3 v3.9.0 // indirect
	github.com/google/gnostic v0.6.9 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
)

require (
	cloud.google.com/go v0.102.1 // indirect
	cloud.google.com/go/storage v1.26.0 // indirect
	contrib.go.opencensus.io/exporter/ocagent v0.7.1-0.20200907061046-05415f1de66d // indirect
	contrib.go.opencensus.io/exporter/prometheus v0.4.0 // indirect
	contrib.go.opencensus.io/exporter/zipkin v0.1.2 // indirect
	github.com/ahmetb/gen-crd-api-reference-docs v0.3.1-0.20210420163308-c1402a70e2f1 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr v1.4.10 // indirect
	github.com/benbjohnson/clock v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/blendle/zapdriver v1.3.1 // indirect
	github.com/census-instrumentation/opencensus-proto v0.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cloudevents/conformance v0.2.0 // indirect
	github.com/cloudevents/sdk-go/observability/opencensus/v2 v2.13.0 // indirect
	github.com/cloudevents/sdk-go/sql/v2 v2.13.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/evanphx/json-patch/v5 v5.6.0 // indirect
	github.com/go-kit/log v0.2.0 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.20.0 // indirect
	github.com/go-openapi/swag v0.19.15 // indirect
	github.com/gobuffalo/flect v0.2.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-github/v27 v27.0.6 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/mako v0.0.0-20190821191249-122f8dcef9e3 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.1.0 // indirect
	github.com/googleapis/gax-go/v2 v2.4.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.0 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pelletier/go-toml/v2 v2.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.14.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/prometheus/statsd_exporter v0.21.0 // indirect
	github.com/rickb777/plural v1.2.2 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/fastuuid v1.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tsenart/vegeta/v12 v12.8.4 // indirect
	github.com/wavesoftware/go-ensure v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/stringprep v1.0.3 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/automaxprocs v1.4.0 // indirect
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/mod v0.8.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/oauth2 v0.0.0-20220822191816-0ebed06d0094 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/term v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	golang.org/x/time v0.0.0-20220224211638-0e9765cccd65 // indirect
	golang.org/x/tools v0.6.0 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
	gomodules.xyz/jsonpatch/v2 v2.2.0 // indirect
	gonum.org/v1/gonum v0.0.0-20190331200053-3d26580ed485 // indirect
	google.golang.org/api v0.94.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220829175752-36a9c930ecbf // indirect
	google.golang.org/grpc v1.49.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/code-generator v0.26.5 // indirect
	k8s.io/gengo v0.0.0-20221011193443-fad74ee6edd9 // indirect
	k8s.io/klog v1.0.0 // indirect
	k8s.io/klog/v2 v2.80.2-0.20221028030830-9ae4992afb54 // indirect
	k8s.io/kube-openapi v0.0.0-20221012153701-172d655c2280 // indirect
	sigs.k8s.io/json v0.0.0-20220713155537-f223a00ba0e2 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
)

replace github.com/cloudevents/sdk-go/sql/v2 => github.com/cloudevents/sdk-go/sql/v2 v2.0.0-20220930150014-52b12276cc4a
