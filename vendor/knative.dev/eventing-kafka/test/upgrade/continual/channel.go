/*
Copyright 2021 The Knative Authors

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

package continual

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	reconcilertesting "knative.dev/eventing-kafka/pkg/channel/consolidated/reconciler/testing"
	testlib "knative.dev/eventing-kafka/test/lib"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
	"knative.dev/eventing/test/lib/duck"
	"knative.dev/eventing/test/lib/resources"
	"knative.dev/eventing/test/upgrade/prober/sut"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

const (
	channelConfigTemplatePath = "test/upgrade/continual/channel-config.toml"
	defaultRetryCount         = 12
	defaultBackoffPolicy      = eventingduckv1.BackoffPolicyExponential
	defaultBackoffDelay       = "PT1S"
)

var (
	defaultChannelType = metav1.TypeMeta{
		APIVersion: "messaging.knative.dev/v1beta1",
		Kind:       "KafkaChannel",
	}
)

// ChannelTestOptions holds test options for KafkaChannel tests.
type ChannelTestOptions struct {
	*TestOptions
	*ReplicationOptions
	*RetryOptions
	*metav1.TypeMeta
}

// ChannelTest tests channel operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func ChannelTest(opts ChannelTestOptions) pkgupgrade.BackgroundOperation {
	opts = opts.withDefaults()
	return continualVerification(
		"ChannelContinualTest",
		opts.TestOptions,
		channelSut(opts),
		channelConfigTemplatePath,
	)
}

// BrokerBackedByChannelTest tests a broker backed by KafkaChannel operation in
// continual manner during the whole upgrade and downgrade process asserting
// that all event are propagated well.
func BrokerBackedByChannelTest(opts ChannelTestOptions) pkgupgrade.BackgroundOperation {
	opts = opts.withDefaults()
	return continualVerification(
		"BrokerBackedByChannelContinualTest",
		opts.TestOptions,
		brokerBackedByChannelSut(opts),
		channelConfigTemplatePath,
	)
}

func (o ChannelTestOptions) withDefaults() ChannelTestOptions {
	cto := o
	if cto.TestOptions == nil {
		cto.TestOptions = &TestOptions{}
	}
	if cto.TypeMeta == nil {
		cto.TypeMeta = &defaultChannelType
	}
	if cto.RetryOptions == nil {
		cto.RetryOptions = defaultRetryOptions()
	}
	if cto.ReplicationOptions == nil {
		cto.ReplicationOptions = defaultReplicationOptions()
	}
	return cto
}

func channelSut(opts ChannelTestOptions) sut.SystemUnderTest {
	return &kafkaChannelSut{
		channelTypeMeta:    opts.TypeMeta,
		ReplicationOptions: opts.ReplicationOptions,
		RetryOptions:       opts.RetryOptions,
	}
}

func defaultReplicationOptions() *ReplicationOptions {
	return &ReplicationOptions{
		NumPartitions:     6,
		ReplicationFactor: 3,
	}
}

func defaultRetryOptions() *RetryOptions {
	return &RetryOptions{
		RetryCount:    defaultRetryCount,
		BackoffPolicy: defaultBackoffPolicy,
		BackoffDelay:  defaultBackoffDelay,
	}
}

func brokerBackedByChannelSut(opts ChannelTestOptions) sut.SystemUnderTest {
	return &brokerBackedByKafkaChannelSut{
		channelTypeMeta:    opts.TypeMeta,
		ReplicationOptions: opts.ReplicationOptions,
		RetryOptions:       opts.RetryOptions,
	}
}

type kafkaChannelSut struct {
	channelTypeMeta *metav1.TypeMeta
	*ReplicationOptions
	*RetryOptions
}

func (k kafkaChannelSut) Deploy(ctx sut.Context, destination duckv1.Destination) interface{} {
	c := ctx.Client
	name := "sut"
	ch := reconcilertesting.NewKafkaChannel(
		name,
		c.Namespace,
		k.RetryOptions.channelOption(),
		k.ReplicationOptions.channelOption(),
	)
	testlib.CreateKafkaChannelV1Beta1OrFail(c, ch)
	metaResource := resources.NewMetaResource(name, c.Namespace, k.channelTypeMeta)
	if err := duck.WaitForResourceReady(c.Dynamic, metaResource); err != nil {
		c.T.Fatal(err)
	}

	var sutUrl *apis.URL
	if u, err := c.GetAddressableURI(name, k.channelTypeMeta); err != nil {
		c.T.Fatal(err)
	} else {
		sutUrl, err = apis.ParseURL(u)
		if err != nil {
			c.T.Fatal(err)
		}
	}

	c.CreateSubscriptionOrFail(
		name, name, k.channelTypeMeta,
		withDestinationForSubscription(&destination),
		k.RetryOptions.subscriptionOption(),
	)

	return sutUrl
}

func withDestinationForSubscription(destination *duckv1.Destination) resources.SubscriptionOption {
	return func(subscription *messagingv1.Subscription) {
		subscription.Spec.Subscriber = destination
	}
}

func (ro RetryOptions) subscriptionOption() resources.SubscriptionOption {
	return func(subscription *messagingv1.Subscription) {
		ensureSubscriptionHasDelivery(subscription)
		r := int32(ro.RetryCount)
		subscription.Spec.Delivery.Retry = &r
		subscription.Spec.Delivery.BackoffPolicy = &ro.BackoffPolicy
		subscription.Spec.Delivery.BackoffDelay = &ro.BackoffDelay
	}
}

func (ro RetryOptions) channelOption() reconcilertesting.KafkaChannelOption {
	return func(channel *v1beta1.KafkaChannel) {
		ensureChannelHasDelivery(channel)
		r := int32(ro.RetryCount)
		channel.Spec.Delivery.Retry = &r
		channel.Spec.Delivery.BackoffPolicy = &ro.BackoffPolicy
		channel.Spec.Delivery.BackoffDelay = &ro.BackoffDelay
	}
}

func (ro ReplicationOptions) channelOption() reconcilertesting.KafkaChannelOption {
	return func(channel *v1beta1.KafkaChannel) {
		channel.Spec.ReplicationFactor = int16(ro.ReplicationFactor)
		channel.Spec.NumPartitions = int32(ro.NumPartitions)
	}
}

func ensureChannelHasDelivery(channel *v1beta1.KafkaChannel) {
	if channel.Spec.Delivery == nil {
		channel.Spec.Delivery = &eventingduckv1.DeliverySpec{}
	}
}

func ensureSubscriptionHasDelivery(subscription *messagingv1.Subscription) {
	if subscription.Spec.Delivery == nil {
		subscription.Spec.Delivery = &eventingduckv1.DeliverySpec{}
	}
}

type brokerBackedByKafkaChannelSut struct {
	*ReplicationOptions
	*RetryOptions
	channelTypeMeta *metav1.TypeMeta
	defaultSut      sut.SystemUnderTest
}

func (b *brokerBackedByKafkaChannelSut) Deploy(
	ctx sut.Context,
	destination duckv1.Destination,
) interface{} {
	b.setKafkaAsDefaultForBroker(ctx)

	b.defaultSut = sut.NewDefault()
	return b.defaultSut.Deploy(ctx, destination)
}

func (b *brokerBackedByKafkaChannelSut) Teardown(ctx sut.Context) {
	if b.defaultSut == nil {
		ctx.T.Fatal("default SUT isn't set!?!")
	}
	if tr, ok := b.defaultSut.(sut.HasTeardown); ok {
		tr.Teardown(ctx)
	}
}

func (b *brokerBackedByKafkaChannelSut) setKafkaAsDefaultForBroker(ctx sut.Context) {
	systemNs := "knative-eventing"
	configmaps := ctx.Client.Kube.CoreV1().ConfigMaps(systemNs)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: systemNs,
			Name:      "config-br-default-channel",
		},
		Data: map[string]string{
			"channelTemplateSpec": fmt.Sprintf(`apiVersion: %s
kind: %s
spec:
  numPartitions: %d
  replicationFactor: %d
  delivery:
    retry: %d
    backoffPolicy: %s
    backoffDelay: %s`,
				b.channelTypeMeta.APIVersion, b.channelTypeMeta.Kind,
				b.NumPartitions, b.ReplicationFactor,
				b.RetryCount, b.BackoffPolicy, b.BackoffDelay,
			),
		},
	}
	cm, err := configmaps.Update(ctx.Ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		ctx.T.Fatal(err)
	}

	ctx.Log.Info("Updated config-br-default-channel in ns knative-eventing"+
		" to eq: ", cm.Data)
}
