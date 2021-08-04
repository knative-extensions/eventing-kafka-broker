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
	"io/ioutil"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
)

const (
	ConfigMapNamespace = "test-namespace-config-map"
	ConfigMapName      = "test-config-cm"

	serviceNamespace = "test-service-namespace"
	serviceName      = "test-service"
	ServiceURL       = "http://test-service.test-service-namespace.svc.cluster.local"

	TriggerUUID = "e7185016-5d98-4b54-84e8-3b1cd4acc6b5"

	SecretResourceVersion = "1234"
	SecretUUID            = "a7185016-5d98-4b54-84e8-3b1cd4acc6b6"
)

var (
	Formats = []string{base.Protobuf, base.Json}
)

func NewService() *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: serviceNamespace,
		},
	}
}

func NewConfigMap(configs *Configs, data []byte) runtime.Object {
	return reconcilertesting.NewConfigMap(
		configs.DataPlaneConfigMapName,
		configs.DataPlaneConfigMapNamespace,
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

func NewConfigMapFromContract(contract *contract.Contract, configs *Configs) runtime.Object {
	var data []byte
	var err error
	if configs.DataPlaneConfigFormat == base.Protobuf {
		data, err = proto.Marshal(contract)
	} else {
		data, err = protojson.Marshal(contract)
	}
	if err != nil {
		panic(err)
	}

	return NewConfigMap(configs, data)
}

func ConfigMapUpdate(configs *Configs, contract *contract.Contract) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "ConfigMap",
		},
		configs.DataPlaneConfigMapNamespace,
		NewConfigMapFromContract(contract, configs),
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
