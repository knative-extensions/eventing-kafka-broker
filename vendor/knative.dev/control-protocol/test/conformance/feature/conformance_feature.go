package feature

import (
	"context"
	"fmt"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/control-protocol/test/conformance/resources/conformance_client"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"

	"knative.dev/control-protocol/test/conformance/resources/conformance_server"
)

func ConformanceFeature(clientImage string, serverImage string) *feature.Feature {
	f := feature.NewFeature()

	client := "client"
	server := "server"

	port := 10000

	f.Setup("Start server", conformance_server.StartPod(server, serverImage, port))
	f.Setup("Wait for server ready", func(ctx context.Context, t feature.T) {
		k8s.WaitForPodRunningOrFail(ctx, t, server)
	})
	f.Setup("Start client", func(ctx context.Context, t feature.T) {
		pod, err := kubeclient.Get(ctx).CoreV1().Pods(environment.FromContext(ctx).Namespace()).Get(ctx, server, metav1.GetOptions{})
		require.NoError(t, err)
		require.NotEmpty(t, pod.Status.PodIP)

		conformance_client.StartJob(client, clientImage, fmt.Sprintf("%s:%d", pod.Status.PodIP, port))(ctx, t)
	})

	f.Stable("Send and receive").Must("Job should succeed", func(ctx context.Context, t feature.T) {
		require.NoError(t, k8s.WaitUntilJobDone(
			ctx,
			kubeclient.Get(ctx),
			environment.FromContext(ctx).Namespace(),
			client,
		))
	}).Must("Pod shouldn't be failed", func(ctx context.Context, t feature.T) {
		pod, err := kubeclient.Get(ctx).CoreV1().Pods(environment.FromContext(ctx).Namespace()).Get(ctx, server, metav1.GetOptions{})
		require.NoError(t, err)
		require.Contains(t, []corev1.PodPhase{corev1.PodRunning, corev1.PodSucceeded}, pod.Status.Phase)
	})

	return f
}
