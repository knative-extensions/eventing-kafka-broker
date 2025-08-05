/*
 * Copyright 2022 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consumergroup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	v1 "k8s.io/client-go/informers/core/v1"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/tools/cache"

	internalsapi "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internalskafkaeventing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/offset"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"

	"knative.dev/eventing/pkg/scheduler"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/filtered"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/system"

	statefulsetscheduler "knative.dev/eventing/pkg/scheduler/statefulset"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internalskafkaeventing/v1alpha1"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/internalskafkaeventing/v1alpha1/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/internalskafkaeventing/v1alpha1/consumergroup"
	cgreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/internalskafkaeventing/v1alpha1/consumergroup"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/counter"

	kedaclient "knative.dev/eventing-kafka-broker/third_party/pkg/client/injection/client"
)

const (
	// KafkaSourceScheduler is the key for the scheduler map for any KafkaSource.
	// Keep this constant lowercase.
	KafkaSourceScheduler = "kafkasource"
	// KafkaTriggerScheduler is the key for the scheduler map for any Kafka Trigger.
	// Keep this constant lowercase.
	KafkaTriggerScheduler = "trigger"
	// KafkaChannelScheduler is the key for the scheduler map for any KafkaChannel.
	// Keep this constant lowercase.
	KafkaChannelScheduler = "kafkachannel"
)

var (
	statefulSetScaleCacheRefreshPeriod = time.Minute * 5
)

type envConfig struct {
	SchedulerRefreshPeriod     int64  `envconfig:"AUTOSCALER_REFRESH_PERIOD" required:"true"`
	PodCapacity                int32  `envconfig:"POD_CAPACITY" required:"true"`
	DispatcherMinReplicas      int32  `envconfig:"DISPATCHERS_MIN_REPLICAS" required:"true"`
	SchedulerPolicyConfigMap   string `envconfig:"SCHEDULER_CONFIG" required:"true"`
	DeSchedulerPolicyConfigMap string `envconfig:"DESCHEDULER_CONFIG" required:"true"`
	AutoscalerConfigMap        string `envconfig:"AUTOSCALER_CONFIG" required:"true"`
}

type SchedulerConfig struct {
	StatefulSetName string
	RefreshPeriod   time.Duration
	Capacity        int32
	MinReplicas     int32
}

func NewController(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
	logger := logging.FromContext(ctx)

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logger.Panicf("unable to process required environment variables: %v", err)
	}

	c := SchedulerConfig{
		RefreshPeriod: time.Duration(env.SchedulerRefreshPeriod) * time.Second,
		Capacity:      env.PodCapacity,
		MinReplicas:   env.DispatcherMinReplicas,
	}

	dispatcherPodInformer := podinformer.Get(ctx, internalsapi.DispatcherLabelSelectorStr)

	schedulers := map[string]Scheduler{
		KafkaSourceScheduler:  createKafkaScheduler(ctx, c, kafkainternals.SourceStatefulSetName, dispatcherPodInformer),
		KafkaTriggerScheduler: createKafkaScheduler(ctx, c, kafkainternals.BrokerStatefulSetName, dispatcherPodInformer),
		KafkaChannelScheduler: createKafkaScheduler(ctx, c, kafkainternals.ChannelStatefulSetName, dispatcherPodInformer),
	}

	mp := otel.GetMeterProvider()
	meter := mp.Meter("knative.dev/eventing-kafka-broker/pkg/reconciler/consumergroup")

	r := &Reconciler{
		SchedulerFunc:                      func(s string) (Scheduler, bool) { sched, ok := schedulers[strings.ToLower(s)]; return sched, ok },
		ConsumerLister:                     consumer.Get(ctx).Lister(),
		InternalsClient:                    internalsclient.Get(ctx).InternalV1alpha1(),
		SecretLister:                       secretinformer.Get(ctx).Lister(),
		ConfigMapLister:                    configmapinformer.Get(ctx).Lister(),
		PodLister:                          dispatcherPodInformer.Lister(),
		KubeClient:                         kubeclient.Get(ctx),
		NameGenerator:                      names.SimpleNameGenerator,
		InitOffsetsFunc:                    offset.InitOffsets,
		SystemNamespace:                    system.Namespace(),
		KafkaFeatureFlags:                  config.DefaultFeaturesConfig(),
		KedaClient:                         kedaclient.Get(ctx),
		AutoscalerConfig:                   env.AutoscalerConfigMap,
		DeleteConsumerGroupMetadataCounter: counter.NewExpiringCounter(ctx),
		InitOffsetLatestInitialOffsetCache: prober.NewLocalExpiringCache[string, prober.Status, struct{}](ctx, 20*time.Minute),
	}

	var err error
	r.ScheduleLatency, err = meter.Int64Histogram(
		"kn.eventing.schedule.latency",
		metric.WithDescription("The latency to schedule the consumers"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(latencyBoundsMs...),
	)
	if err != nil {
		logger.Panicw("failed to initialize kn.eventing.schedule.latency metric", zap.Error(err))
	}

	r.InitializeOffsetLatency, err = meter.Int64Histogram(
		"kn.eventing.initialize_offset.latency",
		metric.WithDescription("The latency to initialize consumer group offsets"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(latencyBoundsMs...),
	)
	if err != nil {
		logger.Panicw("failed to initialize kn.eventing.initialize_offset.latency metric", zap.Error(err))
	}

	r.ReadyReplicas, err = meter.Int64Gauge(
		"kn.eventing.replicas.ready",
		metric.WithDescription("The number of ready replicas"),
	)
	if err != nil {
		logger.Panicw("failed to initialize kn.eventing.replicas.ready metric", zap.Error(err))
	}

	r.ExpectedReplicas, err = meter.Int64Gauge(
		"kn.eventing.replicas.expected",
		metric.WithDescription("The number of expected replicas"),
	)
	if err != nil {
		logger.Panicw("failed to initialize kn.eventing.replicas.expected metric", zap.Error(err))
	}

	clientPool := clientpool.Get(ctx)
	if clientPool == nil {
		r.GetKafkaClusterAdmin = clientpool.DisabledGetKafkaClusterAdminFunc
		r.GetKafkaClient = clientpool.DisabledGetClient
	} else {
		r.GetKafkaClusterAdmin = clientPool.GetClusterAdmin
		r.GetKafkaClient = clientPool.GetClient
	}

	consumerInformer := consumer.Get(ctx)

	consumerGroupInformer := consumergroup.Get(ctx)

	impl := cgreconciler.NewImpl(ctx, r, func(impl *controller.Impl) controller.Options {
		return controller.Options{
			PromoteFunc: func(bkt reconciler.Bucket) {
				for _, value := range schedulers {
					if ss, ok := value.Scheduler.(*statefulsetscheduler.StatefulSetScheduler); ok {
						ss.Promote(bkt, nil)
					}
				}
			},
			DemoteFunc: func(bkt reconciler.Bucket) {
				for _, value := range schedulers {
					if ss, ok := value.Scheduler.(*statefulsetscheduler.StatefulSetScheduler); ok {
						ss.Demote(bkt)
					}
				}
			},
		}
	})

	r.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)
	r.EnqueueKey = func(key string) {
		parts := strings.SplitN(key, string(types.Separator), 3)
		if len(parts) != 2 {
			panic(fmt.Sprintf("Expected <namespace>/<name> format, got %s", key))
		}
		impl.EnqueueKey(types.NamespacedName{Namespace: parts[0], Name: parts[1]})
	}

	configStore := config.NewStore(ctx, func(name string, value *config.KafkaFeatureFlags) {
		r.KafkaFeatureFlags.Reset(value)
		impl.GlobalResync(consumerGroupInformer.Informer())
	})
	configStore.WatchConfigs(watcher)

	consumerGroupInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    impl.Enqueue,
		UpdateFunc: controller.PassNew(impl.Enqueue),
		DeleteFunc: func(obj interface{}) {
			impl.Enqueue(obj)
			if cg, ok := obj.(metav1.Object); ok && cg != nil {
				r.InitOffsetLatestInitialOffsetCache.Expire(keyOf(cg))
			}
		},
	})
	consumerInformer.Informer().AddEventHandler(controller.HandleAll(enqueueConsumerGroupFromConsumer(impl.EnqueueKey)))

	ResyncOnStatefulSetChange(ctx, impl.FilteredGlobalResync, consumerGroupInformer.Informer(), func(obj interface{}) (*kafkainternals.ConsumerGroup, bool) {
		cg, ok := obj.(*kafkainternals.ConsumerGroup)
		return cg, ok
	})

	dispatcherPodInformer.Informer().AddEventHandler(controller.HandleAll(func(obj interface{}) {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return
		}

		kind, ok := kafkainternals.GetOwnerKindFromStatefulSetPrefix(pod.Name)
		if !ok {
			return
		}

		cmName, err := internalsapi.ConfigMapNameFromPod(pod)
		if err != nil {
			logger.Warnw("Failed to get ConfigMap name from pod", zap.String("pod", pod.Name), zap.Error(err))
			return
		}
		if err := r.ensureContractConfigMapExists(ctx, pod, cmName); err != nil {
			logger.Warnw("Failed to ensure ConfigMap for pod exists", zap.String("pod", pod.Name), zap.String("configmap", cmName), zap.Error(err))
			return
		}

		impl.FilteredGlobalResync(
			func(obj interface{}) bool {
				cg, ok := obj.(*kafkainternals.ConsumerGroup)
				if !ok {
					return false
				}

				uf := cg.GetUserFacingResourceRef()
				if uf == nil {
					return false
				}
				if strings.EqualFold(kind, uf.Kind) {
					return true
				}
				return false
			},
			consumerGroupInformer.Informer(),
		)
	}))

	//Todo: ScaledObject informer when KEDA is installed

	return impl
}

func ResyncOnStatefulSetChange(ctx context.Context, filteredResync func(f func(interface{}) bool, si cache.SharedInformer), informer cache.SharedInformer, getConsumerGroupFromObj func(obj interface{}) (*kafkainternals.ConsumerGroup, bool)) {
	systemNamespace := system.Namespace()

	handleResync := func(obj interface{}) {

		ss, ok := obj.(*appsv1.StatefulSet)
		if !ok {
			return
		}

		kind, ok := kafkainternals.GetOwnerKindFromStatefulSetPrefix(ss.GetName())
		if !ok {
			return
		}

		filteredResync(func(i interface{}) bool {
			cg, ok := getConsumerGroupFromObj(i)
			if !ok {
				return false
			}
			for _, owner := range cg.OwnerReferences {
				if strings.EqualFold(owner.Kind, kind) {
					return true
				}
			}

			return false
		}, informer)
	}

	statefulset.Get(ctx).Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			ss := obj.(*appsv1.StatefulSet)
			return ss.GetNamespace() == systemNamespace && kafkainternals.IsKnownStatefulSet(ss.GetName())
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: handleResync,
			UpdateFunc: func(oldObj, newObj interface{}) {
				o, ok := oldObj.(*appsv1.StatefulSet)
				if !ok {
					return
				}
				n, ok := newObj.(*appsv1.StatefulSet)
				if !ok {
					return
				}

				// This should never happen, but we check for nil to be sure.
				if o.Spec.Replicas == nil || n.Spec.Replicas == nil {
					return
				}

				// Only handle when replicas change
				if *o.Spec.Replicas == *n.Spec.Replicas && o.Status.ReadyReplicas == n.Status.ReadyReplicas {
					return
				}

				handleResync(newObj)
			},
			DeleteFunc: handleResync,
		},
	})
}

func enqueueConsumerGroupFromConsumer(enqueue func(name types.NamespacedName)) func(obj interface{}) {
	return func(obj interface{}) {
		c, ok := obj.(*kafkainternals.Consumer)
		if ok {
			or := c.GetConsumerGroup()
			if or != nil {
				enqueue(types.NamespacedName{Namespace: c.GetNamespace(), Name: or.Name})
			}
		}
	}
}

func createKafkaScheduler(ctx context.Context, c SchedulerConfig, ssName string, dispatcherPodInformer v1.PodInformer) Scheduler {
	lister := consumergroup.Get(ctx).Lister()
	return createStatefulSetScheduler(
		ctx,
		SchedulerConfig{
			StatefulSetName: ssName,
			RefreshPeriod:   c.RefreshPeriod,
			Capacity:        c.Capacity,
		},
		func() ([]scheduler.VPod, error) {
			consumerGroups, err := lister.List(labels.SelectorFromSet(getSelectorLabel(ssName)))
			if err != nil {
				return nil, err
			}
			vpods := make([]scheduler.VPod, len(consumerGroups))
			for i := 0; i < len(consumerGroups); i++ {
				vpods[i] = consumerGroups[i]
			}
			return vpods, nil
		},
		dispatcherPodInformer,
	)
}

func getSelectorLabel(ssName string) map[string]string {
	//TODO remove hardcoded kinds
	var selectorLabel map[string]string
	switch ssName {
	case kafkainternals.SourceStatefulSetName:
		selectorLabel = map[string]string{
			kafkainternals.UserFacingResourceLabelSelector: "kafkasource",
		}
	case kafkainternals.BrokerStatefulSetName:
		selectorLabel = map[string]string{
			kafkainternals.UserFacingResourceLabelSelector: "trigger",
		}
	case kafkainternals.ChannelStatefulSetName:
		selectorLabel = map[string]string{
			kafkainternals.UserFacingResourceLabelSelector: "kafkachannel",
		}
	}
	return selectorLabel
}

func createStatefulSetScheduler(ctx context.Context, c SchedulerConfig, lister scheduler.VPodLister, dispatcherPodInformer v1.PodInformer) Scheduler {
	ss, _ := statefulsetscheduler.New(ctx, &statefulsetscheduler.Config{
		StatefulSetNamespace: system.Namespace(),
		StatefulSetName:      c.StatefulSetName,
		ScaleCacheConfig:     scheduler.ScaleCacheConfig{RefreshPeriod: statefulSetScaleCacheRefreshPeriod},
		PodCapacity:          c.Capacity,
		RefreshPeriod:        c.RefreshPeriod,
		Evictor:              newEvictor(ctx, zap.String("kafka.eventing.knative.dev/component", "evictor")).evict,
		VPodLister:           lister,
		PodLister:            dispatcherPodInformer.Lister().Pods(system.Namespace()),
		MinReplicas:          c.MinReplicas,
	})

	return Scheduler{
		Scheduler:       ss,
		SchedulerConfig: c,
	}
}
