/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package lib

import (
	"context"

	"k8s.io/client-go/util/retry"
	testlib "knative.dev/eventing/test/lib"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bindingsv1beta1 "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	channelsv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
)

func CreateKafkaChannelV1Beta1OrFail(c *testlib.Client, kafkaChannel *channelsv1beta1.KafkaChannel) {
	kafkaChannelClientSet, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaChannel client: %v", err)
	}

	kChannels := kafkaChannelClientSet.MessagingV1beta1().KafkaChannels(c.Namespace)
	if createdKafkaChannel, err := kChannels.Create(context.Background(), kafkaChannel, metav1.CreateOptions{}); err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaChannel %q: %v", kafkaChannel.Name, err)
	} else {
		c.Tracker.AddObj(createdKafkaChannel)
	}
}

func GetKafkaChannelV1Beta1OrFail(c *testlib.Client, kafkaChannel string) *channelsv1beta1.KafkaChannel {
	kafkaChannelClientSet, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaChannel client: %v", err)
	}

	kChannels := kafkaChannelClientSet.MessagingV1beta1().KafkaChannels(c.Namespace)
	if kcObj, err := kChannels.Get(context.Background(), kafkaChannel, metav1.GetOptions{}); err != nil {
		c.T.Fatalf("Failed to get v1beta1 KafkaChannel %q: %v", kafkaChannel, err)
	} else {
		return kcObj
	}
	return nil
}

func CreateKafkaSourceV1Beta1OrFail(c *testlib.Client, kafkaSource *sourcesv1beta1.KafkaSource) {
	kafkaSourceClientSet, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaSource client: %v", err)
	}

	kSources := kafkaSourceClientSet.SourcesV1beta1().KafkaSources(c.Namespace)
	if createdKafkaSource, err := kSources.Create(context.Background(), kafkaSource, metav1.CreateOptions{}); err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaSource %q: %v", kafkaSource.Name, err)
	} else {
		c.Tracker.AddObj(createdKafkaSource)
	}
}

func GetKafkaSourceV1Beta1OrFail(c *testlib.Client, kafkaSource string) *sourcesv1beta1.KafkaSource {
	kafkaSourceClientSet, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaSource client: %v", err)
	}

	kSources := kafkaSourceClientSet.SourcesV1beta1().KafkaSources(c.Namespace)
	if ksObj, err := kSources.Get(context.Background(), kafkaSource, metav1.GetOptions{}); err != nil {
		c.T.Fatalf("Failed to get v1beta1 KafkaSource %q: %v", kafkaSource, err)
	} else {
		return ksObj
	}
	return nil
}

func UpdateKafkaSourceV1Beta1OrFail(c *testlib.Client, kafkaSource *sourcesv1beta1.KafkaSource) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestKafkaSource := GetKafkaSourceV1Beta1OrFail(c, kafkaSource.Name)
		kafkaSource.Spec.DeepCopyInto(&latestKafkaSource.Spec)
		kafkaSourceClientSet, err := kafkaclientset.NewForConfig(c.Config)
		if err != nil {
			c.T.Fatalf("Failed to create v1beta1 KafkaSource client: %v", err)
		}

		kSources := kafkaSourceClientSet.SourcesV1beta1().KafkaSources(c.Namespace)
		_, err = kSources.Update(context.Background(), latestKafkaSource, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		c.T.Fatalf("Failed to update v1beta1 KafkaSource %q: %v", kafkaSource.Name, err)
	}
}

func CreateKafkaBindingV1Beta1OrFail(c *testlib.Client, kafkaBinding *bindingsv1beta1.KafkaBinding) {
	kafkaBindingClientSet, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaBinding client: %v", err)
	}

	kBindings := kafkaBindingClientSet.BindingsV1beta1().KafkaBindings(c.Namespace)
	if createdKafkaBinding, err := kBindings.Create(context.Background(), kafkaBinding, metav1.CreateOptions{}); err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaBinding %q: %v", kafkaBinding.Name, err)
	} else {
		c.Tracker.AddObj(createdKafkaBinding)
	}
}
