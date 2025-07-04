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
	apiskedav1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/apis/keda/v1alpha1"
	versioned "knative.dev/eventing-kafka-broker/third_party/pkg/client/clientset/versioned"
	internalinterfaces "knative.dev/eventing-kafka-broker/third_party/pkg/client/informers/externalversions/internalinterfaces"
	kedav1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/client/listers/keda/v1alpha1"
)

// ScaledJobInformer provides access to a shared informer and lister for
// ScaledJobs.
type ScaledJobInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() kedav1alpha1.ScaledJobLister
}

type scaledJobInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewScaledJobInformer constructs a new informer for ScaledJob type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewScaledJobInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredScaledJobInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredScaledJobInformer constructs a new informer for ScaledJob type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredScaledJobInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.KedaV1alpha1().ScaledJobs(namespace).List(context.Background(), options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.KedaV1alpha1().ScaledJobs(namespace).Watch(context.Background(), options)
			},
			ListWithContextFunc: func(ctx context.Context, options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.KedaV1alpha1().ScaledJobs(namespace).List(ctx, options)
			},
			WatchFuncWithContext: func(ctx context.Context, options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.KedaV1alpha1().ScaledJobs(namespace).Watch(ctx, options)
			},
		},
		&apiskedav1alpha1.ScaledJob{},
		resyncPeriod,
		indexers,
	)
}

func (f *scaledJobInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredScaledJobInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *scaledJobInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&apiskedav1alpha1.ScaledJob{}, f.defaultInformer)
}

func (f *scaledJobInformer) Lister() kedav1alpha1.ScaledJobLister {
	return kedav1alpha1.NewScaledJobLister(f.Informer().GetIndexer())
}
