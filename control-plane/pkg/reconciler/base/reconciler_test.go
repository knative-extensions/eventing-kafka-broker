package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/pkg/tracker"
)

func TestTrackConfigMap(t *testing.T) {

	r := &Reconciler{
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
