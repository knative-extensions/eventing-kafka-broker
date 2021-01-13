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
