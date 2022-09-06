/*
 * Copyright 2022 The Knative Authors
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

package kafka

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kedav1alpha1 "knative.dev/eventing-autoscaler-keda/third_party/pkg/apis/keda/v1alpha1"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"

	"knative.dev/eventing-autoscaler-keda/pkg/reconciler/keda"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
)

const (
	defaultKafkaLagThreshold = 10
)

func GenerateScaleTarget(cg *kafkainternals.ConsumerGroup) *kedav1alpha1.ScaleTarget {
	return &kedav1alpha1.ScaleTarget{
		Name:       cg.Name,
		APIVersion: kafkainternals.SchemeGroupVersion.String(),
		Kind:       "ConsumerGroup",
	}
}

func GenerateScaleTriggers(cg *kafkainternals.ConsumerGroup, triggerAuthentication *kedav1alpha1.TriggerAuthentication) ([]kedav1alpha1.ScaleTriggers, error) {
	triggers := []kedav1alpha1.ScaleTriggers{}
	bootstrapServers := cg.Spec.Template.Spec.Configs.Configs["bootstrap.servers"]
	consumerGroup := cg.Spec.Template.Spec.Configs.Configs["group.id"]

	lagThreshold, err := keda.GetInt32ValueFromMap(cg.Annotations, keda.KedaAutoscalingKafkaLagThreshold, defaultKafkaLagThreshold)
	if err != nil {
		return nil, err
	}

	allowIdleConsumers := "false"
	if cg.Status.Placements != nil {
		allowIdleConsumers = "true"
	}

	for _, topic := range cg.Spec.Template.Spec.Topics {
		triggerMetadata := map[string]string{
			"bootstrapServers":   bootstrapServers,
			"consumerGroup":      consumerGroup,
			"topic":              topic,
			"lagThreshold":       strconv.Itoa(int(*lagThreshold)),
			"allowIdleConsumers": allowIdleConsumers,
		}

		trigger := kedav1alpha1.ScaleTriggers{
			Type:              "kafka",
			Metadata:          triggerMetadata,
			AuthenticationRef: &kedav1alpha1.ScaledObjectAuthRef{},
		}

		if triggerAuthentication != nil {
			trigger.AuthenticationRef.Name = triggerAuthentication.Name
		}

		triggers = append(triggers, trigger)
	}

	return triggers, nil
}

func GenerateTriggerAuthentication(cg *kafkainternals.ConsumerGroup, saslType *string) (*kedav1alpha1.TriggerAuthentication, *corev1.Secret, error) {

	secretTargetRefs := []kedav1alpha1.AuthSecretTargetRef{}
	var secret corev1.Secret

	triggerAuth := &kedav1alpha1.TriggerAuthentication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-trigger-auth", cg.Name),
			Namespace: cg.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(cg),
			},
			Labels: map[string]string{
				//TODO
			},
		},
		Spec: kedav1alpha1.TriggerAuthenticationSpec{
			SecretTargetRef: secretTargetRefs,
		},
	}

	if cg.Spec.Template.Spec.Auth.NetSpec != nil {
		secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-secret", cg.Name),
				Namespace: cg.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					*kmeta.NewControllerRef(cg),
				},
			},
			Data:       make(map[string][]byte),
			StringData: make(map[string]string),
		}

		if cg.Spec.Template.Spec.Auth.NetSpec.SASL.Enable {

			if saslType != nil {
				switch *saslType {
				case "SCRAM-SHA-256":
					secret.StringData["sasl"] = "scram_sha256"
				case "SCRAM-SHA-512":
					secret.StringData["sasl"] = "scram_sha512"
				case "PLAIN":
					secret.StringData["sasl"] = "plaintext"
				default:
					return nil, nil, fmt.Errorf("SASL type value %q is not supported", *saslType)
				}
			} else {
				secret.StringData["sasl"] = "plaintext" //default
			}

			sasl := kedav1alpha1.AuthSecretTargetRef{Parameter: "sasl", Name: secret.Name, Key: "sasl"}

			username := kedav1alpha1.AuthSecretTargetRef{
				Parameter: "username",
				Name:      cg.Spec.Template.Spec.Auth.NetSpec.SASL.User.SecretKeyRef.Name,
				Key:       cg.Spec.Template.Spec.Auth.NetSpec.SASL.User.SecretKeyRef.Key,
			}
			password := kedav1alpha1.AuthSecretTargetRef{
				Parameter: "password",
				Name:      cg.Spec.Template.Spec.Auth.NetSpec.SASL.Password.SecretKeyRef.Name,
				Key:       cg.Spec.Template.Spec.Auth.NetSpec.SASL.Password.SecretKeyRef.Key,
			}

			secretTargetRefs = append(secretTargetRefs, sasl, username, password)
			triggerAuth.Spec.SecretTargetRef = secretTargetRefs
		}

		if cg.Spec.Template.Spec.Auth.NetSpec.TLS.Enable {
			secret.StringData["tls"] = "enable"
			tls := kedav1alpha1.AuthSecretTargetRef{Parameter: "tls", Name: secret.Name, Key: "tls"}
			secretTargetRefs = append(secretTargetRefs, tls)

			if cg.Spec.Template.Spec.Auth.NetSpec.TLS.CACert.SecretKeyRef != nil {
				ca := kedav1alpha1.AuthSecretTargetRef{
					Parameter: "ca",
					Name:      cg.Spec.Template.Spec.Auth.NetSpec.TLS.CACert.SecretKeyRef.Name,
					Key:       cg.Spec.Template.Spec.Auth.NetSpec.TLS.CACert.SecretKeyRef.Key,
				}

				secretTargetRefs = append(secretTargetRefs, ca)
			}

			if cg.Spec.Template.Spec.Auth.NetSpec.TLS.Cert.SecretKeyRef != nil {
				cert := kedav1alpha1.AuthSecretTargetRef{
					Parameter: "cert",
					Name:      cg.Spec.Template.Spec.Auth.NetSpec.TLS.Cert.SecretKeyRef.Name,
					Key:       cg.Spec.Template.Spec.Auth.NetSpec.TLS.Cert.SecretKeyRef.Key,
				}
				secretTargetRefs = append(secretTargetRefs, cert)
			}

			if cg.Spec.Template.Spec.Auth.NetSpec.TLS.Key.SecretKeyRef != nil {
				key := kedav1alpha1.AuthSecretTargetRef{
					Parameter: "key",
					Name:      cg.Spec.Template.Spec.Auth.NetSpec.TLS.Key.SecretKeyRef.Name,
					Key:       cg.Spec.Template.Spec.Auth.NetSpec.TLS.Key.SecretKeyRef.Key,
				}
				secretTargetRefs = append(secretTargetRefs, key)
			}
			triggerAuth.Spec.SecretTargetRef = secretTargetRefs
		}
	}

	if cg.Spec.Template.Spec.Auth.AuthSpec != nil && cg.Spec.Template.Spec.Auth.AuthSpec.Secret.Ref.Name != "" {
		host := kedav1alpha1.AuthSecretTargetRef{
			Parameter: "host",
			Name:      cg.Spec.Template.Spec.Auth.AuthSpec.Secret.Ref.Name,
			Key:       "", //TODO: add key
		}
		secretTargetRefs = append(secretTargetRefs, host)
		triggerAuth.Spec.SecretTargetRef = secretTargetRefs
		return triggerAuth, nil, nil
	}

	return triggerAuth, &secret, nil
}

// scaleObjectCreated makes a new reconciler event with event type Normal, and
// reason ScaledObjectCreated.
func ScaleObjectCreated(namespace, name string) reconciler.Event {
	return reconciler.NewEvent(corev1.EventTypeNormal, "ScaledObjectCreated", "ScaledObject created: \"%s/%s\"", namespace, name)
}

// scaleObjectUpdated makes a new reconciler event with event type Normal, and
// reason ScaledObjectUpdated.
func ScaleObjectUpdated(namespace, name string) reconciler.Event {
	return reconciler.NewEvent(corev1.EventTypeNormal, "ScaledObjectUpdated", "ScaledObject updated: \"%s/%s\"", namespace, name)
}

// scaleObjectDeploymentFailed makes a new reconciler event with event type Warning, and
// reason ScaleObjectDeploymentFailed.
func ScaleObjectDeploymentFailed(namespace, name string, err error) reconciler.Event {
	return reconciler.NewEvent(corev1.EventTypeWarning, "ScaleObjectDeploymentFailed", "ScaledObject deployment failed to: \"%s/%s\", %w", namespace, name, err)
}
