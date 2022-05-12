/*
Copyright 2020 The Knative Authors

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

package resources

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
)

type DispatcherServiceBuilder struct {
	service *corev1.Service
	args    *DispatcherServiceArgs
}

type DispatcherServiceArgs struct {
	DispatcherNamespace string
	ServiceAnnotations  map[string]string
	ServiceLabels       map[string]string
}

// NewDispatcherServiceBuilder returns a builder which builds from scratch a dispatcher service.
// Intended to be used when creating the dispatcher service for the first time.
func NewDispatcherServiceBuilder() *DispatcherServiceBuilder {
	b := &DispatcherServiceBuilder{}
	b.service = dispatcherServiceTemplate()
	return b
}

// NewDispatcherServiceBuilderFromService returns a builder which builds a dispatcher service from the given service.
// Intended to be used when updating an existing dispatcher service.
func NewDispatcherServiceBuilderFromService(s *corev1.Service) *DispatcherServiceBuilder {
	b := &DispatcherServiceBuilder{}
	b.service = s
	return b
}

func (b *DispatcherServiceBuilder) WithArgs(args *DispatcherServiceArgs) *DispatcherServiceBuilder {
	b.args = args
	return b
}

func (b *DispatcherServiceBuilder) Build() *corev1.Service {
	b.service.ObjectMeta.Namespace = b.args.DispatcherNamespace
	b.service.ObjectMeta.Annotations = commonconfig.JoinStringMaps(b.service.Annotations, b.args.ServiceAnnotations)
	b.service.ObjectMeta.Labels = commonconfig.JoinStringMaps(b.service.Labels, b.args.ServiceLabels)
	return b.service
}

func dispatcherServiceTemplate() *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   dispatcherName,
			Labels: dispatcherLabels,
		},
		Spec: corev1.ServiceSpec{
			Selector: dispatcherLabels,
			Ports: []corev1.ServicePort{
				{
					Name:       "http-dispatcher",
					Protocol:   corev1.ProtocolTCP,
					Port:       80,
					TargetPort: intstr.IntOrString{IntVal: 8080},
				},
			},
		},
	}
}
