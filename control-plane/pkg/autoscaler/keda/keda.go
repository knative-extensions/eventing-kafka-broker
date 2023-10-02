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

package keda

import (
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/third_party/pkg/client/clientset/versioned"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler"
	kedav1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/apis/keda/v1alpha1"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	// AutoscalerClass is the KEDA autoscaler class.
	AutoscalerClass = "keda.autoscaling.knative.dev"
)

func GenerateScaleTarget(cg *kafkainternals.ConsumerGroup) *kedav1alpha1.ScaleTarget {
	return &kedav1alpha1.ScaleTarget{
		Name:       cg.Name,
		APIVersion: kafkainternals.SchemeGroupVersion.String(),
		Kind:       "ConsumerGroup",
	}
}

func GenerateScaleTriggers(cg *kafkainternals.ConsumerGroup, triggerAuthentication *kedav1alpha1.TriggerAuthentication, aconfig autoscaler.AutoscalerConfig) ([]kedav1alpha1.ScaleTriggers, error) {
	triggers := make([]kedav1alpha1.ScaleTriggers, 0, len(cg.Spec.Template.Spec.Topics))
	bootstrapServers := cg.Spec.Template.Spec.Configs.Configs[kafka.BootstrapServersConfigMapKey]
	consumerGroup := cg.Spec.Template.Spec.Configs.Configs[kafka.GroupIDConfigMapKey]

	lagThreshold, err := GetInt32ValueFromMap(cg.Annotations, autoscaler.AutoscalingLagThreshold, aconfig.AutoscalerDefaults[autoscaler.AutoscalingLagThreshold])
	if err != nil {
		return nil, err
	}

	activationLagThreshold, err := GetInt32ValueFromMap(cg.Annotations, autoscaler.AutoscalingActivationLagThreshold, aconfig.AutoscalerDefaults[autoscaler.AutoscalingActivationLagThreshold])
	if err != nil {
		return nil, err
	}

	allowIdleConsumers := "false"
	if cg.Status.Placements != nil {
		allowIdleConsumers = "true"
	}

	for _, topic := range cg.Spec.Template.Spec.Topics {
		triggerMetadata := map[string]string{
			"bootstrapServers":       bootstrapServers,
			"consumerGroup":          consumerGroup,
			"topic":                  topic,
			"lagThreshold":           strconv.Itoa(int(*lagThreshold)),
			"activationLagThreshold": strconv.Itoa(int(*activationLagThreshold)),
			"allowIdleConsumers":     allowIdleConsumers,
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

func GenerateTriggerAuthentication(cg *kafkainternals.ConsumerGroup, secretData map[string][]byte) (*kedav1alpha1.TriggerAuthentication, *corev1.Secret, error) {

	secretTargetRefs := make([]kedav1alpha1.AuthSecretTargetRef, 0, 8)

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cg.Name,
			Namespace: cg.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(cg),
			},
		},
		Data:       secretData,
		StringData: make(map[string]string),
	}

	saslType := retrieveSaslTypeIfPresent(cg, secret)

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

	triggerAuth := &kedav1alpha1.TriggerAuthentication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cg.Name,
			Namespace: cg.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(cg),
			},
			Labels: map[string]string{
				//TODO: may need to add labels like eventing-autoscaler-keda/pkg/reconciler/broker/resources/triggerauthentication.go#L39-L40
			},
		},
		Spec: kedav1alpha1.TriggerAuthenticationSpec{
			SecretTargetRef: secretTargetRefs,
		},
	}

	if cg.Spec.Template.Spec.Auth.NetSpec != nil {

		if cg.Spec.Template.Spec.Auth.NetSpec.SASL.Enable {
			sasl := kedav1alpha1.AuthSecretTargetRef{Parameter: "sasl", Name: secret.Name, Key: "sasl"}
			secretTargetRefs = append(secretTargetRefs, sasl)

			secretTargetRefs = addAuthSecretTargetRef("username", cg.Spec.Template.Spec.Auth.NetSpec.SASL.User, secretTargetRefs)
			secretTargetRefs = addAuthSecretTargetRef("password", cg.Spec.Template.Spec.Auth.NetSpec.SASL.Password, secretTargetRefs)

			triggerAuth.Spec.SecretTargetRef = secretTargetRefs
		}

		if cg.Spec.Template.Spec.Auth.NetSpec.TLS.Enable {
			secret.StringData["tls"] = "enable"
			tls := kedav1alpha1.AuthSecretTargetRef{Parameter: "tls", Name: secret.Name, Key: "tls"}
			secretTargetRefs = append(secretTargetRefs, tls)

			secretTargetRefs = addAuthSecretTargetRef("ca", cg.Spec.Template.Spec.Auth.NetSpec.TLS.CACert, secretTargetRefs)
			secretTargetRefs = addAuthSecretTargetRef("cert", cg.Spec.Template.Spec.Auth.NetSpec.TLS.Cert, secretTargetRefs)
			secretTargetRefs = addAuthSecretTargetRef("key", cg.Spec.Template.Spec.Auth.NetSpec.TLS.Key, secretTargetRefs)

			triggerAuth.Spec.SecretTargetRef = secretTargetRefs
		}
	}

	if cg.Spec.Template.Spec.Auth.SecretSpec != nil && cg.Spec.Template.Spec.Auth.SecretSpec.Ref.Name != "" {

		if saslType != nil { //SASL enabled
			sasl := kedav1alpha1.AuthSecretTargetRef{Parameter: "sasl", Name: secret.Name, Key: "sasl"}
			secretTargetRefs = append(secretTargetRefs, sasl)

			if protocolValue, ok := secret.Data[security.ProtocolKey]; ok && string(protocolValue) != "" {
				user := kedav1alpha1.AuthSecretTargetRef{Parameter: "username", Name: secret.Name, Key: security.SaslUserKey}
				secretTargetRefs = append(secretTargetRefs, user)
			} else {
				username := kedav1alpha1.AuthSecretTargetRef{Parameter: "username", Name: secret.Name, Key: security.SaslUsernameKey}
				secretTargetRefs = append(secretTargetRefs, username)
			}

			password := kedav1alpha1.AuthSecretTargetRef{Parameter: "password", Name: secret.Name, Key: security.SaslPasswordKey}
			secretTargetRefs = append(secretTargetRefs, password)
		}

		if caCertValue, ok := secret.Data[security.CaCertificateKey]; ok && string(caCertValue) != "" { // TLS enabled
			secret.StringData["tls"] = "enable"
			tls := kedav1alpha1.AuthSecretTargetRef{Parameter: "tls", Name: secret.Name, Key: "tls"}
			secretTargetRefs = append(secretTargetRefs, tls)

			ca := kedav1alpha1.AuthSecretTargetRef{Parameter: "ca", Name: secret.Name, Key: security.CaCertificateKey}
			secretTargetRefs = append(secretTargetRefs, ca)

			cert := kedav1alpha1.AuthSecretTargetRef{Parameter: "cert", Name: secret.Name, Key: security.UserCertificate}
			secretTargetRefs = append(secretTargetRefs, cert)

			key := kedav1alpha1.AuthSecretTargetRef{Parameter: "key", Name: secret.Name, Key: security.UserKey}
			secretTargetRefs = append(secretTargetRefs, key)
		}

		triggerAuth.Spec.SecretTargetRef = secretTargetRefs
	}

	return triggerAuth, &secret, nil
}

func retrieveSaslTypeIfPresent(cg *kafkainternals.ConsumerGroup, secret corev1.Secret) *string {
	if cg.Spec.Template.Spec.Auth.NetSpec != nil && cg.Spec.Template.Spec.Auth.NetSpec.SASL.Enable && cg.Spec.Template.Spec.Auth.NetSpec.SASL.Type.SecretKeyRef != nil {
		secretKeyRefKey := cg.Spec.Template.Spec.Auth.NetSpec.SASL.Type.SecretKeyRef.Key
		if saslTypeValue, ok := secret.Data[secretKeyRefKey]; ok {
			return pointer.String(string(saslTypeValue))
		}
	}

	if cg.Spec.Template.Spec.Auth.SecretSpec != nil && cg.Spec.Template.Spec.Auth.SecretSpec.Ref != nil {
		if saslTypeValue, ok := secret.Data[security.SaslTypeLegacy]; ok {
			return pointer.String(string(saslTypeValue))
		}
	}
	return nil
}

func addAuthSecretTargetRef(parameter string, secretKeyRef v1beta1.SecretValueFromSource, secretTargetRefs []kedav1alpha1.AuthSecretTargetRef) []kedav1alpha1.AuthSecretTargetRef {
	if secretKeyRef.SecretKeyRef == nil || secretKeyRef.SecretKeyRef.Name == "" || secretKeyRef.SecretKeyRef.Key == "" {
		return secretTargetRefs
	}

	ref := kedav1alpha1.AuthSecretTargetRef{
		Parameter: parameter,
		Name:      secretKeyRef.SecretKeyRef.Name,
		Key:       secretKeyRef.SecretKeyRef.Key,
	}

	secretTargetRefs = append(secretTargetRefs, ref)
	return secretTargetRefs
}

func SetAutoscalingAnnotations(objAnnotations map[string]string) map[string]string {
	if objAnnotations != nil {
		cgAnnotations := make(map[string]string, len(objAnnotations))
		setAnnotation(objAnnotations, autoscaler.AutoscalingClassAnnotation, cgAnnotations)
		setAnnotation(objAnnotations, autoscaler.AutoscalingMinScaleAnnotation, cgAnnotations)
		setAnnotation(objAnnotations, autoscaler.AutoscalingMaxScaleAnnotation, cgAnnotations)
		setAnnotation(objAnnotations, autoscaler.AutoscalingPollingIntervalAnnotation, cgAnnotations)
		setAnnotation(objAnnotations, autoscaler.AutoscalingCooldownPeriodAnnotation, cgAnnotations)
		setAnnotation(objAnnotations, autoscaler.AutoscalingLagThreshold, cgAnnotations)
		setAnnotation(objAnnotations, autoscaler.AutoscalingActivationLagThreshold, cgAnnotations)
		return cgAnnotations
	}
	return nil
}

func setAnnotation(objAnnotations map[string]string, key string, cgAnnotations map[string]string) {
	value, ok := objAnnotations[key]
	if !ok || value == "" {
		return
	}
	cgAnnotations[key] = value
}

func IsEnabled(ctx context.Context, features *config.KafkaFeatureFlags, client versioned.Interface, object metav1.Object) bool {
	if !features.IsControllerAutoscalerEnabled() {
		return false
	}

	v, ok := object.GetAnnotations()[autoscaler.AutoscalingClassAnnotation]
	if ok && v == autoscaler.AutoscalingClassDisabledAnnotationValue {
		return false
	}
	if ok && len(v) > 0 && v != AutoscalerClass {
		return false
	}

	// TODO: code below failing unit tests with err: "panic: interface conversion: testing.ActionImpl is not testing.GetAction: missing method GetName"
	/*if err := discovery.ServerSupportsVersion(r.KubeClient.Discovery(), keda.KedaSchemeGroupVersion); err == nil {
		 return true
	 }*/

	if _, err := client.KedaV1alpha1().ScaledObjects(object.GetNamespace()).List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		logging.FromContext(ctx).Debug("KEDA not installed, failed to list ScaledObjects")
		return false
	}

	return true
}
