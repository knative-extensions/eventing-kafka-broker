/*
 * Copyright 2020 The Knative Authors
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

package testing

import (
	corev1 "k8s.io/api/core/v1"
	fakeapiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/runtime"
	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	fakeeventingclientset "knative.dev/eventing/pkg/client/clientset/versioned/fake"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/pkg/reconciler/testing"

	eventingkafkachannels "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	eventingkafkasources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	fakeeventingkafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned/fake"
	eventingkafkachannelslisters "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	eventingkafkasourceslisters "knative.dev/eventing-kafka/pkg/client/listers/sources/v1beta1"

	eventingkafkabroker "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	fakeeventingkafkabrokerclientset "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/fake"
	eventingkafkabrokerlisters "knative.dev/eventing-kafka-broker/control-plane/pkg/client/listers/eventing/v1alpha1"
)

var clientSetSchemes = []func(*runtime.Scheme) error{
	fakeeventingclientset.AddToScheme,
	fakekubeclientset.AddToScheme,
	fakeapiextensionsclientset.AddToScheme,
	fakeeventingkafkabrokerclientset.AddToScheme,
	fakeeventingkafkaclientset.AddToScheme,
}

type Listers struct {
	sorter testing.ObjectSorter
}

func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()

	for _, addTo := range clientSetSchemes {
		_ = addTo(scheme)
	}
	return scheme
}

func newListers(objs []runtime.Object) *Listers {

	scheme := newScheme()

	ls := Listers{
		sorter: testing.NewObjectSorter(scheme),
	}

	ls.sorter.AddObjects(objs...)

	return &ls
}

func (l *Listers) GetAllObjects() []runtime.Object {
	all := l.GetKubeObjects()
	all = append(all, l.GetEventingObjects()...)
	return all
}

func (l *Listers) GetKubeObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakekubeclientset.AddToScheme)
}

func (l *Listers) GetEventingObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakeeventingclientset.AddToScheme)
}

func (l *Listers) GetEventingKafkaBrokerObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakeeventingkafkabrokerclientset.AddToScheme)
}

func (l *Listers) GetEventingKafkaObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakeeventingkafkaclientset.AddToScheme)
}

func (l *Listers) GetBrokerLister() eventinglisters.BrokerLister {
	return eventinglisters.NewBrokerLister(l.indexerFor(&eventing.Broker{}))
}

func (l *Listers) GetPodLister() corelisters.PodLister {
	return corelisters.NewPodLister(l.indexerFor(&corev1.Pod{}))
}

func (l *Listers) GetSecretLister() corelisters.SecretLister {
	return corelisters.NewSecretLister(l.indexerFor(&corev1.Secret{}))
}

func (l *Listers) GetTriggerLister() eventinglisters.TriggerLister {
	return eventinglisters.NewTriggerLister(l.indexerFor(&eventing.Trigger{}))
}

func (l *Listers) GetConfigMapLister() corelisters.ConfigMapLister {
	return corelisters.NewConfigMapLister(l.indexerFor(&corev1.ConfigMap{}))
}

func (l *Listers) GetKafkaSinkLister() eventingkafkabrokerlisters.KafkaSinkLister {
	return eventingkafkabrokerlisters.NewKafkaSinkLister(l.indexerFor(&eventingkafkabroker.KafkaSink{}))
}

func (l *Listers) GetKafkaSourceLister() eventingkafkasourceslisters.KafkaSourceLister {
	return eventingkafkasourceslisters.NewKafkaSourceLister(l.indexerFor(&eventingkafkasources.KafkaSource{}))
}

func (l *Listers) GetKafkaChannelLister() eventingkafkachannelslisters.KafkaChannelLister {
	return eventingkafkachannelslisters.NewKafkaChannelLister(l.indexerFor(&eventingkafkachannels.KafkaChannel{}))
}

func (l *Listers) indexerFor(obj runtime.Object) cache.Indexer {
	return l.sorter.IndexerForObjectType(obj)
}
