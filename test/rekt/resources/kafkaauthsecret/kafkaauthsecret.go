/*
 * Copyright 2023 The Knative Authors
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

package kafkaauthsecret

import (
	"context"

	"knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
	"knative.dev/reconciler-test/pkg/resources/secret"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Install(name string, opts ...manifest.CfgFn) feature.StepFn {
	return secret.Install(name, opts...)
}

func WithPlaintextData() manifest.CfgFn {
	return secret.WithStringData(map[string]string{
		"protocol": "PLAINTEXT",
	})
}

func WithSslData(ctx context.Context) manifest.CfgFn {
	caSecret, err := client.Get(ctx).CoreV1().Secrets(pkg.KafkaClusterNamespace).Get(ctx, pkg.CaSecretName, metav1.GetOptions{})
	if err != nil {
		panic(err)
	}

	tlsUserSecret, err := client.Get(ctx).CoreV1().Secrets(pkg.KafkaClusterNamespace).Get(ctx, pkg.TlsUserSecretName, metav1.GetOptions{})
	if err != nil {
		panic(err)
	}

	return secret.WithData(map[string][]byte{
		"protocol": []byte("SSL"),
		"ca.crt":   caSecret.Data["ca.crt"],
		"user.crt": tlsUserSecret.Data["user.crt"],
		"user.key": tlsUserSecret.Data["user.key"],
	})
}

func WithTlsNoAuthData(ctx context.Context) manifest.CfgFn {
	caSecret, err := client.Get(ctx).CoreV1().Secrets(pkg.KafkaClusterNamespace).Get(ctx, pkg.CaSecretName, metav1.GetOptions{})
	if err != nil {
		panic(err)
	}

	return secret.WithData(map[string][]byte{
		"protocol":  []byte("SSL"),
		"ca.crt":    caSecret.Data["ca.crt"],
		"user.skip": []byte("true"),
	})
}

func WithSaslPlaintextScram512Data(ctx context.Context) manifest.CfgFn {
	saslUserSecret, err := client.Get(ctx).CoreV1().Secrets(pkg.KafkaClusterNamespace).Get(ctx, pkg.SaslUserSecretName, metav1.GetOptions{})
	if err != nil {
		panic(err)
	}

	return secret.WithData(map[string][]byte{
		"protocol":       []byte("SASL_PLAINTEXT"),
		"sasl.mechanism": []byte("SCRAM-SHA-512"),
		"user":           []byte(pkg.SaslUserSecretName),
		"password":       saslUserSecret.Data["password"],
	})
}

func WithSslSaslScram512Data(ctx context.Context) manifest.CfgFn {
	caSecret, err := client.Get(ctx).CoreV1().Secrets(pkg.KafkaClusterNamespace).Get(ctx, pkg.CaSecretName, metav1.GetOptions{})
	if err != nil {
		panic(err)
	}

	saslUserSecret, err := client.Get(ctx).CoreV1().Secrets(pkg.KafkaClusterNamespace).Get(ctx, pkg.SaslUserSecretName, metav1.GetOptions{})
	if err != nil {
		panic(err)
	}

	return secret.WithData(map[string][]byte{
		"protocol":       []byte("SASL_SSL"),
		"sasl.mechanism": []byte("SCRAM-SHA-512"),
		"ca.crt":         caSecret.Data["ca.crt"],
		"user":           []byte(pkg.SaslUserSecretName),
		"password":       saslUserSecret.Data["password"],
	})
}

func WithRestrictedSslSaslScram512Data(ctx context.Context) manifest.CfgFn {
	caSecret, err := client.Get(ctx).CoreV1().Secrets(pkg.KafkaClusterNamespace).Get(ctx, pkg.CaSecretName, metav1.GetOptions{})
	if err != nil {
		panic(err)
	}

	saslRestrictedUserSecret, err := client.Get(ctx).CoreV1().Secrets(pkg.KafkaClusterNamespace).Get(ctx, pkg.SaslRestrictedUserSecretName, metav1.GetOptions{})
	if err != nil {
		panic(err)
	}

	return secret.WithData(map[string][]byte{
		"protocol":       []byte("SASL_SSL"),
		"sasl.mechanism": []byte("SCRAM-SHA-512"),
		"ca.crt":         caSecret.Data["ca.crt"],
		"user":           []byte(pkg.SaslRestrictedUserSecretName),
		"password":       saslRestrictedUserSecret.Data["password"],
	})
}
