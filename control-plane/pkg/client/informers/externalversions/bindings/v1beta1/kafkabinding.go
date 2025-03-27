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

package v1beta1

import (
	context "context"
	time "time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
	apisbindingsv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	versioned "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned"
	internalinterfaces "knative.dev/eventing-kafka-broker/control-plane/pkg/client/informers/externalversions/internalinterfaces"
	bindingsv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/listers/bindings/v1beta1"
)

// KafkaBindingInformer provides access to a shared informer and lister for
// KafkaBindings.
type KafkaBindingInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() bindingsv1beta1.KafkaBindingLister
}

type kafkaBindingInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewKafkaBindingInformer constructs a new informer for KafkaBinding type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewKafkaBindingInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredKafkaBindingInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredKafkaBindingInformer constructs a new informer for KafkaBinding type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredKafkaBindingInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.BindingsV1beta1().KafkaBindings(namespace).List(context.TODO(), options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.BindingsV1beta1().KafkaBindings(namespace).Watch(context.TODO(), options)
			},
		},
		&apisbindingsv1beta1.KafkaBinding{},
		resyncPeriod,
		indexers,
	)
}

func (f *kafkaBindingInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredKafkaBindingInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *kafkaBindingInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&apisbindingsv1beta1.KafkaBinding{}, f.defaultInformer)
}

func (f *kafkaBindingInformer) Lister() bindingsv1beta1.KafkaBindingLister {
	return bindingsv1beta1.NewKafkaBindingLister(f.Informer().GetIndexer())
}
