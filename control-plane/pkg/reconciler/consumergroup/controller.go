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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/tools/cache"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/offset"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"

	"knative.dev/eventing/pkg/scheduler"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	nodeinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/node"
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
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup"
	cgreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumergroup"
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
	SchedulerPolicyConfigMap   string `envconfig:"SCHEDULER_CONFIG" required:"true"`
	DeSchedulerPolicyConfigMap string `envconfig:"DESCHEDULER_CONFIG" required:"true"`
	AutoscalerConfigMap        string `envconfig:"AUTOSCALER_CONFIG" required:"true"`
}

type SchedulerConfig struct {
	StatefulSetName   string
	RefreshPeriod     time.Duration
	Capacity          int32
	SchedulerPolicy   *scheduler.SchedulerPolicy
	DeSchedulerPolicy *scheduler.SchedulerPolicy
}

func NewController(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
	logger := logging.FromContext(ctx)

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logger.Panicf("unable to process required environment variables: %v", err)
	}

	c := SchedulerConfig{
		RefreshPeriod:     time.Duration(env.SchedulerRefreshPeriod) * time.Second,
		Capacity:          env.PodCapacity,
		SchedulerPolicy:   schedulerPolicyFromConfigMapOrFail(ctx, env.SchedulerPolicyConfigMap),
		DeSchedulerPolicy: schedulerPolicyFromConfigMapOrFail(ctx, env.DeSchedulerPolicyConfigMap),
	}

	schedulers := map[string]Scheduler{
		KafkaSourceScheduler:  createKafkaScheduler(ctx, c, kafkainternals.SourceStatefulSetName),
		KafkaTriggerScheduler: createKafkaScheduler(ctx, c, kafkainternals.BrokerStatefulSetName),
		KafkaChannelScheduler: createKafkaScheduler(ctx, c, kafkainternals.ChannelStatefulSetName),
	}

	clientPool := clientpool.Get(ctx)

	dispatcherPodInformer := podinformer.Get(ctx, eventing.DispatcherLabelSelectorStr)

	r := &Reconciler{
		SchedulerFunc:                      func(s string) (Scheduler, bool) { sched, ok := schedulers[strings.ToLower(s)]; return sched, ok },
		ConsumerLister:                     consumer.Get(ctx).Lister(),
		InternalsClient:                    internalsclient.Get(ctx).InternalV1alpha1(),
		SecretLister:                       secretinformer.Get(ctx).Lister(),
		ConfigMapLister:                    configmapinformer.Get(ctx).Lister(),
		PodLister:                          dispatcherPodInformer.Lister(),
		KubeClient:                         kubeclient.Get(ctx),
		NameGenerator:                      names.SimpleNameGenerator,
		GetKafkaClient:                     clientPool.GetClient,
		InitOffsetsFunc:                    offset.InitOffsets,
		SystemNamespace:                    system.Namespace(),
		GetKafkaClusterAdmin:               clientPool.GetClusterAdmin,
		KafkaFeatureFlags:                  config.DefaultFeaturesConfig(),
		KedaClient:                         kedaclient.Get(ctx),
		AutoscalerConfig:                   env.AutoscalerConfigMap,
		DeleteConsumerGroupMetadataCounter: counter.NewExpiringCounter(ctx),
		InitOffsetLatestInitialOffsetCache: prober.NewLocalExpiringCache[string, prober.Status, struct{}](ctx, 20*time.Minute),
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

		cmName, err := eventing.ConfigMapNameFromPod(pod)
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

func createKafkaScheduler(ctx context.Context, c SchedulerConfig, ssName string) Scheduler {
	lister := consumergroup.Get(ctx).Lister()
	return createStatefulSetScheduler(
		ctx,
		SchedulerConfig{
			StatefulSetName:   ssName,
			RefreshPeriod:     c.RefreshPeriod,
			Capacity:          c.Capacity,
			SchedulerPolicy:   c.SchedulerPolicy,
			DeSchedulerPolicy: c.DeSchedulerPolicy,
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

func createStatefulSetScheduler(ctx context.Context, c SchedulerConfig, lister scheduler.VPodLister) Scheduler {
	ss, _ := statefulsetscheduler.New(ctx, &statefulsetscheduler.Config{
		StatefulSetNamespace: system.Namespace(),
		StatefulSetName:      c.StatefulSetName,
		ScaleCacheConfig:     scheduler.ScaleCacheConfig{RefreshPeriod: statefulSetScaleCacheRefreshPeriod},
		PodCapacity:          c.Capacity,
		RefreshPeriod:        c.RefreshPeriod,
		SchedulerPolicy:      scheduler.MAXFILLUP,
		SchedPolicy:          c.SchedulerPolicy,
		DeschedPolicy:        c.DeSchedulerPolicy,
		Evictor:              newEvictor(ctx, zap.String("kafka.eventing.knative.dev/component", "evictor")).evict,
		VPodLister:           lister,
		NodeLister:           nodeinformer.Get(ctx).Lister(),
	})

	return Scheduler{
		Scheduler:       ss,
		SchedulerConfig: c,
	}
}

// schedulerPolicyFromConfigMapOrFail reads predicates and priorities data from configMap
func schedulerPolicyFromConfigMapOrFail(ctx context.Context, configMapName string) *scheduler.SchedulerPolicy {
	p, err := schedulerPolicyFromConfigMap(ctx, configMapName)
	if err != nil {
		logging.FromContext(ctx).Fatal(zap.Error(err))
	}
	return p
}

// schedulerPolicyFromConfigMap reads predicates and priorities data from configMap
func schedulerPolicyFromConfigMap(ctx context.Context, configMapName string) (*scheduler.SchedulerPolicy, error) {
	policyConfigMap, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("couldn't get scheduler policy config map %s/%s: %v", system.Namespace(), configMapName, err)
	}

	logger := logging.FromContext(ctx).
		Desugar().
		With(zap.String("configmap", configMapName))
	policy := &scheduler.SchedulerPolicy{}

	preds, found := policyConfigMap.Data["predicates"]
	if !found {
		return nil, fmt.Errorf("missing policy config map %s/%s value at key predicates", system.Namespace(), configMapName)
	}
	if err := json.NewDecoder(strings.NewReader(preds)).Decode(&policy.Predicates); err != nil {
		return nil, fmt.Errorf("invalid policy %v: %v", preds, err)
	}

	priors, found := policyConfigMap.Data["priorities"]
	if !found {
		return nil, fmt.Errorf("missing policy config map value at key priorities")
	}
	if err := json.NewDecoder(strings.NewReader(priors)).Decode(&policy.Priorities); err != nil {
		return nil, fmt.Errorf("invalid policy %v: %v", preds, err)
	}

	if errs := validatePolicy(policy); errs != nil {
		return nil, multierr.Combine(err)
	}

	logger.Info("Schedulers policy registration", zap.Any("policy", policy))

	return policy, nil
}

func validatePolicy(policy *scheduler.SchedulerPolicy) []error {
	var validationErrors []error

	for _, priority := range policy.Priorities {
		if priority.Weight < scheduler.MinWeight || priority.Weight > scheduler.MaxWeight {
			validationErrors = append(validationErrors, fmt.Errorf("priority %s should have a positive weight applied to it or it has overflown", priority.Name))
		}
	}
	return validationErrors
}
