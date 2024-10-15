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

	testlib "knative.dev/eventing/test/lib"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	channelsv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/messaging/v1beta1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"
	kafkaclientset "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned"
)

func CreateKafkaChannelV1Beta1OrFail(c *testlib.Client, kafkaChannel *channelsv1beta1.KafkaChannel) {
	client, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaChannel client: %v", err)
	}

	err = c.RetryWebhookErrors(func(i int) error {
		createdKafkaChannel, err := client.MessagingV1beta1().KafkaChannels(c.Namespace).Create(context.Background(), kafkaChannel, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		c.Tracker.AddObj(createdKafkaChannel)
		return nil
	})
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaChannel %q: %v", kafkaChannel.Name, err)
	}
}

func GetKafkaChannelV1Beta1OrFail(c *testlib.Client, kafkaChannel string) *channelsv1beta1.KafkaChannel {
	client, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaChannel client: %v", err)
	}

	var kcObj *channelsv1beta1.KafkaChannel
	err = c.RetryWebhookErrors(func(i int) error {
		kcObj, err = client.MessagingV1beta1().KafkaChannels(c.Namespace).Get(context.Background(), kafkaChannel, metav1.GetOptions{})
		return err
	})
	if err != nil {
		c.T.Fatalf("Failed to get v1beta1 KafkaChannel %q: %v", kafkaChannel, err)
	}
	return kcObj
}

func CreateKafkaSourceOrFail(c *testlib.Client, kafkaSource *sources.KafkaSource) {
	client, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create KafkaSource client: %v", err)
	}

	err = c.RetryWebhookErrors(func(i int) error {
		createdKafkaSource, err := client.SourcesV1().KafkaSources(c.Namespace).Create(context.Background(), kafkaSource, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		c.Tracker.AddObj(createdKafkaSource)
		return nil
	})
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaSource %q: %v", kafkaSource.Name, err)
	}
}

func GetKafkaSourceOrFail(c *testlib.Client, kafkaSource string) *sources.KafkaSource {
	client, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaSource client: %v", err)
	}

	var ksObj *sources.KafkaSource
	err = c.RetryWebhookErrors(func(i int) error {
		ksObj, err = client.SourcesV1().KafkaSources(c.Namespace).Get(context.Background(), kafkaSource, metav1.GetOptions{})
		return err
	})
	if err != nil {
		c.T.Fatalf("Failed to get v1beta1 KafkaSource %q: %v", kafkaSource, err)
	}
	return ksObj
}

func UpdateKafkaSourceOrFail(c *testlib.Client, kafkaSource *sources.KafkaSource) {
	err := c.RetryWebhookErrors(func(i int) error {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latestKafkaSource := GetKafkaSourceOrFail(c, kafkaSource.Name)
			kafkaSource.Spec.DeepCopyInto(&latestKafkaSource.Spec)
			kafkaSourceClientSet, err := kafkaclientset.NewForConfig(c.Config)
			if err != nil {
				c.T.Fatalf("Failed to create v1beta1 KafkaSource client: %v", err)
			}

			kSources := kafkaSourceClientSet.SourcesV1().KafkaSources(c.Namespace)
			_, err = kSources.Update(context.Background(), latestKafkaSource, metav1.UpdateOptions{})
			return err
		})
	})
	if err != nil {
		c.T.Fatalf("Failed to update v1beta1 KafkaSource %q: %v", kafkaSource.Name, err)
	}
}
