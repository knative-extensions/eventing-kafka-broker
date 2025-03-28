/*
 * Copyright 2021 The Knative Authors
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

// Code generated by informer-gen. DO NOT EDIT.

package v1alpha1

import (
	context "context"
	time "time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
	apisinternalskafkaeventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internalskafkaeventing/v1alpha1"
	versioned "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned"
	internalinterfaces "knative.dev/eventing-kafka-broker/control-plane/pkg/client/informers/externalversions/internalinterfaces"
	internalskafkaeventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/listers/internalskafkaeventing/v1alpha1"
)

// ConsumerGroupInformer provides access to a shared informer and lister for
// ConsumerGroups.
type ConsumerGroupInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() internalskafkaeventingv1alpha1.ConsumerGroupLister
}

type consumerGroupInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewConsumerGroupInformer constructs a new informer for ConsumerGroup type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewConsumerGroupInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredConsumerGroupInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredConsumerGroupInformer constructs a new informer for ConsumerGroup type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredConsumerGroupInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.InternalV1alpha1().ConsumerGroups(namespace).List(context.TODO(), options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.InternalV1alpha1().ConsumerGroups(namespace).Watch(context.TODO(), options)
			},
		},
		&apisinternalskafkaeventingv1alpha1.ConsumerGroup{},
		resyncPeriod,
		indexers,
	)
}

func (f *consumerGroupInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredConsumerGroupInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *consumerGroupInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&apisinternalskafkaeventingv1alpha1.ConsumerGroup{}, f.defaultInformer)
}

func (f *consumerGroupInformer) Lister() internalskafkaeventingv1alpha1.ConsumerGroupLister {
	return internalskafkaeventingv1alpha1.NewConsumerGroupLister(f.Informer().GetIndexer())
}
