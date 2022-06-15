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

package base_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	"knative.dev/pkg/logging"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/tracker"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

func TestIsReceiverRunning(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	addRunningPod(podinformer.Get(ctx).Informer().GetStore(), kubeclient.Get(ctx), base.BrokerReceiverLabel)

	r := &base.Reconciler{
		PodLister:     podinformer.Get(ctx).Lister(),
		ReceiverLabel: base.BrokerReceiverLabel,
	}

	require.True(t, r.IsReceiverRunning("ns"))
}

func TestIsReceiverNotRunning(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	r := &base.Reconciler{
		PodLister:     podinformer.Get(ctx).Lister(),
		ReceiverLabel: base.BrokerReceiverLabel,
	}

	require.False(t, r.IsReceiverRunning("ns"))
}

func TestIsDispatcherRunning(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	label := "dispatcher"

	addRunningPod(podinformer.Get(ctx).Informer().GetStore(), kubeclient.Get(ctx), label)

	r := &base.Reconciler{
		PodLister:       podinformer.Get(ctx).Lister(),
		DispatcherLabel: label,
	}

	require.True(t, r.IsDispatcherRunning("ns"))
}

func TestIsDispatcherNotRunning(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	r := &base.Reconciler{
		PodLister:       podinformer.Get(ctx).Lister(),
		DispatcherLabel: base.BrokerDispatcherLabel,
	}

	require.False(t, r.IsDispatcherRunning("ns"))
}

func TestGetOrCreateDataPlaneConfigMap(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	r := &base.Reconciler{
		KubeClient: kubeclient.Get(ctx),
	}

	cm, err := r.GetOrCreateDataPlaneConfigMap(ctx, "knative-eventing")
	require.Nil(t, err)
	require.NotNil(t, cm)
}

func TestGetDataPlaneConfigMapDataEmptyConfigMap(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	r := &base.Reconciler{
		KubeClient: kubeclient.Get(ctx),
	}

	cm := &corev1.ConfigMap{}

	ct, err := r.GetDataPlaneConfigMapData(logging.FromContext(ctx).Desugar(), cm)
	require.Nil(t, err)
	require.NotNil(t, ct)
	require.Equal(t, uint64(0), ct.Generation)
	require.Len(t, ct.Resources, 0)
}

func TestGetDataPlaneConfigMapData(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	r := &base.Reconciler{
		KubeClient:            kubeclient.Get(ctx),
		DataPlaneConfigFormat: base.Json,
	}

	ct := &contract.Contract{}
	ct.Resources = append(ct.Resources, &contract.Resource{Uid: "123"})

	b, err := protojson.Marshal(ct)
	require.Nil(t, err)

	cm := &corev1.ConfigMap{
		BinaryData: map[string][]byte{
			base.ConfigMapDataKey: b,
		},
	}

	got, err := r.GetDataPlaneConfigMapData(logging.FromContext(ctx).Desugar(), cm)
	require.Nil(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Resources, len(ct.Resources))

	ctStr, err := protojson.Marshal(ct)
	require.Nil(t, err)
	gotStr, err := protojson.Marshal(got)
	require.Nil(t, err)

	if diff := cmp.Diff(ctStr, gotStr); diff != "" {
		t.Fatal("(-want, +got)", diff)
	}
}

func TestUpdateDataPlaneConfigMap(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "name",
		},
		BinaryData: map[string][]byte{base.ConfigMapDataKey: []byte("")},
	}

	_, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(cm.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	require.Nil(t, err)

	r := &base.Reconciler{
		KubeClient:            kubeclient.Get(ctx),
		DataPlaneConfigFormat: base.Json,
	}

	ct := &contract.Contract{}
	ct.Resources = append(ct.Resources, &contract.Resource{Uid: "123"})

	err = r.UpdateDataPlaneConfigMap(ctx, ct, cm)
	require.Nil(t, err)
}

func TestGetDataPlaneConfigMapDataCorrupted(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	r := &base.Reconciler{
		KubeClient:            kubeclient.Get(ctx),
		DataPlaneConfigFormat: base.Json,
	}

	cm := &corev1.ConfigMap{
		BinaryData: map[string][]byte{
			base.ConfigMapDataKey: []byte("corrupted"),
		},
	}

	got, err := r.GetDataPlaneConfigMapData(logging.FromContext(ctx).Desugar(), cm)
	require.NotNil(t, err)
	require.Equal(t, uint64(0), got.Generation)
}

func TestUpdateReceiverPodAnnotation(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	addRunningPod(podinformer.Get(ctx).Informer().GetStore(), kubeclient.Get(ctx), base.SinkReceiverLabel)

	r := &base.Reconciler{
		PodLister:     podinformer.Get(ctx).Lister(),
		KubeClient:    kubeclient.Get(ctx),
		ReceiverLabel: base.SinkReceiverLabel,
	}

	err := r.UpdateReceiverPodsAnnotation(ctx, "ns", logging.FromContext(ctx).Desugar(), 1)
	require.Nil(t, err)
}

func TestUpdateDispatcherPodAnnotation(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	label := "dispatcher"

	addRunningPod(podinformer.Get(ctx).Informer().GetStore(), kubeclient.Get(ctx), label)

	r := &base.Reconciler{
		PodLister:       podinformer.Get(ctx).Lister(),
		KubeClient:      kubeclient.Get(ctx),
		DispatcherLabel: label,
	}

	err := r.UpdateDispatcherPodsAnnotation(ctx, "ns", logging.FromContext(ctx).Desugar(), 1)
	require.Nil(t, err)
}

func TestTrackConfigMap(t *testing.T) {

	r := &base.Reconciler{
		ConfigMapTracker: tracker.New(func(name types.NamespacedName) {}, time.Second),
	}

	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "my-namespace",
			Name:      "my-name",
		},
	}
	err := r.TrackConfigMap(cm, &eventing.Broker{})
	assert.Nil(t, err)
}

func TestTrackSecret(t *testing.T) {

	r := &base.Reconciler{
		SecretTracker: tracker.New(func(name types.NamespacedName) {}, time.Second),
	}

	cm := &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "my-namespace",
			Name:      "my-name",
		},
	}
	err := r.TrackSecret(cm, &eventing.Broker{})
	assert.Nil(t, err)
}

func TestOnDeleteObserver(t *testing.T) {

}

func addRunningPod(store cache.Store, kc kubernetes.Interface, label string) {
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod",
			Namespace: "ns",
			Labels:    map[string]string{"app": label},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	if err := store.Add(pod); err != nil {
		panic(err)
	}
	if _, err := kc.CoreV1().Pods("ns").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		panic(err)
	}
}
