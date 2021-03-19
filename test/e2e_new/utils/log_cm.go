package utils

import (
	"context"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/knative"
)

// LogContractConfigMap logs the broker config map
func LogContractConfigMap(ctx context.Context, t feature.T) {
	LogConfigMap("kafka-broker-brokers-triggers", knative.KnativeNamespaceFromContext(ctx))(ctx, t)
}

// LogConfigMap logs the provided config map
func LogConfigMap(name string, namespace string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		cm, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)

		t.Logf("Config map %s/%s", cm.Name, cm.Namespace)
		for k, v := range cm.Data {
			t.Logf("%s: %s", k, v)
		}
		for k, v := range cm.BinaryData {
			t.Logf("%s: %s", k, string(v))
		}
	}
}
