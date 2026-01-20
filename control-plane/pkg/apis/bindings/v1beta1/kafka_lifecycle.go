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

package v1beta1

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/apis/duck"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/tracker"
)

var kfbCondSet = apis.NewLivingConditionSet()

// GetGroupVersionKind returns the GroupVersionKind.
func (kb *KafkaBinding) GetGroupVersionKind() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("KafkaBinding")
}

// GetConditionSet retrieves the condition set for this resource. Implements the KRShaped interface.
func (kb *KafkaBinding) GetConditionSet() apis.ConditionSet {
	return kfbCondSet
}

// GetUntypedSpec implements apis.HasSpec
func (kb *KafkaBinding) GetUntypedSpec() interface{} {
	return kb.Spec
}

// GetSubject implements psbinding.Bindable
func (kb *KafkaBinding) GetSubject() tracker.Reference {
	return kb.Spec.Subject
}

// GetBindingStatus implements psbinding.Bindable
func (kb *KafkaBinding) GetBindingStatus() duck.BindableStatus {
	return &kb.Status
}

// SetObservedGeneration implements psbinding.BindableStatus
func (kbs *KafkaBindingStatus) SetObservedGeneration(gen int64) {
	kbs.ObservedGeneration = gen
}

// InitializeConditions populates the KafkaBindingStatus's conditions field
// with all of its conditions configured to Unknown.
func (kbs *KafkaBindingStatus) InitializeConditions() {
	kfbCondSet.Manage(kbs).InitializeConditions()
}

// MarkBindingUnavailable marks the KafkaBinding's Ready condition to False with
// the provided reason and message.
func (kbs *KafkaBindingStatus) MarkBindingUnavailable(reason, message string) {
	kfbCondSet.Manage(kbs).MarkFalse(KafkaBindingConditionReady, reason, message)
}

// MarkBindingAvailable marks the KafkaBinding's Ready condition to True.
func (kbs *KafkaBindingStatus) MarkBindingAvailable() {
	kfbCondSet.Manage(kbs).MarkTrue(KafkaBindingConditionReady)
}

// Do implements psbinding.Bindable
func (kb *KafkaBinding) Do(ctx context.Context, ps *duckv1.WithPod) {
	// First undo so that we can just unconditionally append below.
	kb.Undo(ctx, ps)

	spec := ps.Spec.Template.Spec
	for i := range spec.InitContainers {
		spec.InitContainers[i].Env = append(spec.InitContainers[i].Env, corev1.EnvVar{
			Name:  "KAFKA_BOOTSTRAP_SERVERS",
			Value: strings.Join(kb.Spec.BootstrapServers, ","),
		})
		if kb.Spec.Net.SASL.Enable {
			spec.InitContainers[i].Env = append(spec.InitContainers[i].Env, corev1.EnvVar{
				Name:  "KAFKA_NET_SASL_ENABLE",
				Value: "true",
			}, corev1.EnvVar{
				Name: "KAFKA_NET_SASL_USER",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.SASL.User.SecretKeyRef,
				},
			}, corev1.EnvVar{
				Name: "KAFKA_NET_SASL_PASSWORD",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.SASL.Password.SecretKeyRef,
				},
			}, corev1.EnvVar{
				Name: "KAFKA_NET_SASL_TYPE",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.SASL.Type.SecretKeyRef,
				},
			})
		}
		if kb.Spec.Net.TLS.Enable {
			spec.InitContainers[i].Env = append(spec.InitContainers[i].Env, corev1.EnvVar{
				Name:  "KAFKA_NET_TLS_ENABLE",
				Value: "true",
			}, corev1.EnvVar{
				Name: "KAFKA_NET_TLS_CERT",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.TLS.Cert.SecretKeyRef,
				},
			}, corev1.EnvVar{
				Name: "KAFKA_NET_TLS_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.TLS.Key.SecretKeyRef,
				},
			}, corev1.EnvVar{
				Name: "KAFKA_NET_TLS_CA_CERT",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.TLS.CACert.SecretKeyRef,
				},
			})
		}
	}

	for i := range spec.Containers {
		spec.Containers[i].Env = append(spec.Containers[i].Env, corev1.EnvVar{
			Name:  "KAFKA_BOOTSTRAP_SERVERS",
			Value: strings.Join(kb.Spec.BootstrapServers, ","),
		})

		if kb.Spec.Net.SASL.Enable {
			spec.Containers[i].Env = append(spec.Containers[i].Env, corev1.EnvVar{
				Name:  "KAFKA_NET_SASL_ENABLE",
				Value: "true",
			}, corev1.EnvVar{
				Name: "KAFKA_NET_SASL_USER",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.SASL.User.SecretKeyRef,
				},
			}, corev1.EnvVar{
				Name: "KAFKA_NET_SASL_PASSWORD",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.SASL.Password.SecretKeyRef,
				},
			}, corev1.EnvVar{
				Name: "KAFKA_NET_SASL_TYPE",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.SASL.Type.SecretKeyRef,
				},
			})
		}
		if kb.Spec.Net.TLS.Enable {
			spec.Containers[i].Env = append(spec.Containers[i].Env, corev1.EnvVar{
				Name:  "KAFKA_NET_TLS_ENABLE",
				Value: "true",
			}, corev1.EnvVar{
				Name: "KAFKA_NET_TLS_CERT",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.TLS.Cert.SecretKeyRef,
				},
			}, corev1.EnvVar{
				Name: "KAFKA_NET_TLS_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.TLS.Key.SecretKeyRef,
				},
			}, corev1.EnvVar{
				Name: "KAFKA_NET_TLS_CA_CERT",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: kb.Spec.Net.TLS.CACert.SecretKeyRef,
				},
			})
		}
	}
}

func (kb *KafkaBinding) Undo(ctx context.Context, ps *duckv1.WithPod) {
	spec := ps.Spec.Template.Spec

	for i, c := range spec.InitContainers {
		if len(c.Env) == 0 {
			continue
		}
		env := make([]corev1.EnvVar, 0, len(spec.InitContainers[i].Env))
		for j, ev := range c.Env {
			switch ev.Name {
			case "KAFKA_NET_TLS_ENABLE", "KAFKA_NET_TLS_CERT", "KAFKA_NET_TLS_KEY", "KAFKA_NET_TLS_CA_CERT",
				"KAFKA_NET_SASL_ENABLE", "KAFKA_NET_SASL_USER", "KAFKA_NET_SASL_PASSWORD", "KAFKA_NET_SASL_TYPE",
				"KAFKA_BOOTSTRAP_SERVERS":

				continue
			default:
				env = append(env, spec.InitContainers[i].Env[j])
			}
		}
		spec.InitContainers[i].Env = env
	}

	for i, c := range spec.Containers {
		if len(c.Env) == 0 {
			continue
		}
		env := make([]corev1.EnvVar, 0, len(spec.Containers[i].Env))
		for j, ev := range c.Env {
			switch ev.Name {
			case "KAFKA_NET_TLS_ENABLE", "KAFKA_NET_TLS_CERT", "KAFKA_NET_TLS_KEY", "KAFKA_NET_TLS_CA_CERT",
				"KAFKA_NET_SASL_ENABLE", "KAFKA_NET_SASL_USER", "KAFKA_NET_SASL_PASSWORD", "KAFKA_NET_SASL_TYPE",
				"KAFKA_BOOTSTRAP_SERVERS":
				continue
			default:
				env = append(env, spec.Containers[i].Env[j])
			}
		}
		spec.Containers[i].Env = env
	}
}
