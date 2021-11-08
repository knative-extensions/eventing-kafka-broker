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

package testing

import (
	"fmt"
	"io/ioutil"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	ConfigMapNamespace = "test-namespace-config-map"
	ConfigMapName      = "test-config-cm"

	ServiceNamespace = "test-service-namespace"
	ServiceName      = "test-service"

	TriggerUUID = "e7185016-5d98-4b54-84e8-3b1cd4acc6b5"

	SecretResourceVersion = "1234"
	SecretUUID            = "a7185016-5d98-4b54-84e8-3b1cd4acc6b6"
)

var (
	Formats = []string{base.Protobuf, base.Json}

	ServiceURL = ServiceURLFrom(ServiceNamespace, ServiceName)
)

func NewService(mutations ...func(*corev1.Service)) *corev1.Service {
	s := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ServiceName,
			Namespace: ServiceNamespace,
		},
	}
	for _, mut := range mutations {
		mut(s)
	}
	return s
}

func WithServiceNamespace(ns string) func(s *corev1.Service) {
	return func(s *corev1.Service) {
		s.Namespace = ns
	}
}

func ServiceURLFrom(ns, name string) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local", name, ns)
}

func NewConfigMap(env *config.Env, data []byte) runtime.Object {
	return reconcilertesting.NewConfigMap(
		env.DataPlaneConfigMapName,
		env.DataPlaneConfigMapNamespace,
		func(configMap *corev1.ConfigMap) {
			if configMap.BinaryData == nil {
				configMap.BinaryData = make(map[string][]byte, 1)
			}
			if data == nil {
				data = []byte("")
			}
			configMap.BinaryData[base.ConfigMapDataKey] = data
		},
	)
}

func NewConfigMapFromContract(contract *contract.Contract, env *config.Env) runtime.Object {
	var data []byte
	var err error
	if env.DataPlaneConfigFormat == base.Protobuf {
		data, err = proto.Marshal(contract)
	} else {
		data, err = protojson.Marshal(contract)
	}
	if err != nil {
		panic(err)
	}

	return NewConfigMap(env, data)
}

func ConfigMapUpdate(env *config.Env, contract *contract.Contract) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "ConfigMap",
		},
		env.DataPlaneConfigMapNamespace,
		NewConfigMapFromContract(contract, env),
	)
}

func NewSSLSecret(ns, name string) *corev1.Secret {

	ca, userKey, userCert := loadCerts()

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            name,
			ResourceVersion: SecretResourceVersion,
			UID:             SecretUUID,
		},
		Data: map[string][]byte{
			security.ProtocolKey:      []byte(security.ProtocolSSL),
			security.CaCertificateKey: ca,
			security.UserKey:          userKey,
			security.UserCertificate:  userCert,
		},
	}
}

func loadCerts() (ca, userKey, userCert []byte) {
	ca, err := ioutil.ReadFile("testdata/ca.crt")
	if err != nil {
		panic(err)
	}

	userKey, err = ioutil.ReadFile("testdata/user.key")
	if err != nil {
		panic(err)
	}

	userCert, err = ioutil.ReadFile("testdata/user.crt")
	if err != nil {
		panic(err)
	}

	return ca, userKey, userCert
}

type KRShapedOption func(obj duckv1.KRShaped)

// TODO: rename funcs to StatusXXX

func ConfigParsed(obj duckv1.KRShaped) {
	obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrue(base.ConditionConfigParsed)
}

func ConfigNotParsed(obj duckv1.KRShaped, reason string) {
	obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(base.ConditionConfigParsed, reason, "")
}

func ConfigMapUpdatedReady(env *config.Env) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrueWithReason(
			base.ConditionConfigMapUpdated,
			fmt.Sprintf("Config map %s updated", env.DataPlaneConfigMapAsString()),
			"",
		)
	}
}

func TopicReadyWithName(topic string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrueWithReason(
			base.ConditionTopicReady,
			fmt.Sprintf("Topic %s created", topic),
			"",
		)
	}
}

func TopicReadyWithOwner(topic, owner string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrueWithReason(
			base.ConditionTopicReady,
			fmt.Sprintf("Topic %s (owner %s)", topic, owner),
			"",
		)
	}
}

func ControllerOwnsTopic(topicOwner string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		allocateStatusAnnotations(obj)
		obj.GetStatus().Annotations[base.TopicOwnerAnnotation] = topicOwner
	}
}

func TopicNotPresentErr(topic string, err error) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(
			base.ConditionTopicReady,
			base.ReasonTopicNotPresentOrInvalid,
			fmt.Sprintf("topics %v: "+SinkNotPresentErrFormat, []string{topic}, []string{topic}, err),
		)
	}
}

func FailedToCreateTopic(topicName string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(
			base.ConditionTopicReady,
			fmt.Sprintf("Failed to create topic: %s", topicName),
			"%v",
			fmt.Errorf("failed to create topic"),
		)
	}
}

//func InitialOffsetsCommitted(obj duckv1.KRShaped) {
//	obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrue(base.ConditionInitialOffsetsCommitted)
//}

func DataPlaneAvailable(obj duckv1.KRShaped) {
	obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrue(base.ConditionDataPlaneAvailable)
}

func DataPlaneNotAvailable(obj duckv1.KRShaped) {
	obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(
		base.ConditionDataPlaneAvailable,
		base.ReasonDataPlaneNotAvailable,
		base.MessageDataPlaneNotAvailable,
	)
}

func allocateStatusAnnotations(obj duckv1.KRShaped) {
	if obj.GetStatus().Annotations == nil {
		obj.GetStatus().Annotations = make(map[string]string, 1)
	}
}
