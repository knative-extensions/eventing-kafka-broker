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
	"os"
	"time"

	"github.com/magiconair/properties"
	appsv1 "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"

	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	kafkaeventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	ConfigMapNamespace = "test-namespace-config-map"
	ConfigMapName      = "test-config-cm"

	ServiceNamespace = "test-service-namespace"
	ServiceName      = "test-service"

	Service2Name = "test-service-2"

	TriggerUUID = "e7185016-5d98-4b54-84e8-3b1cd4acc6b5"

	SecretResourceVersion = "1234"
	SecretUUID            = "a7185016-5d98-4b54-84e8-3b1cd4acc6b6"

	SystemNamespace = "knative-eventing"

	DispatcherPodUUID = "a7185016-5d98-4b54-84e8-3b1cd4acc6bp"
)

var (
	Formats = []string{base.Protobuf, base.Json}

	ServiceURL         = ServiceURLFrom(ServiceNamespace, ServiceName)
	ServiceHTTPSURL    = ServiceHTTPSURLFrom(ServiceNamespace, ServiceName)
	ServiceDestination = ServiceURLDestination(ServiceNamespace, ServiceName)
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

func NewService2(mutations ...func(*corev1.Service)) *corev1.Service {
	s := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      Service2Name,
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

func ServiceHTTPSURLFrom(ns, name string) string {
	return fmt.Sprintf("https://%s.%s.svc.cluster.local", name, ns)
}

func ServiceURLDestination(ns, name string) *duckv1.Destination {
	uri, _ := apis.ParseURL(ServiceURL)
	return &duckv1.Destination{URI: uri}
}

func NewServiceAccount(namespace string, name string, mutations ...func(account *corev1.ServiceAccount)) *corev1.ServiceAccount {
	s := &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ServiceAccount",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	for _, mut := range mutations {
		mut(s)
	}
	return s
}

func NewRoleBinding(namespace string, name string, mutations ...func(account *rbacv1.RoleBinding)) *rbacv1.RoleBinding {
	s := &rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{
			Kind:       "RoleBinding",
			APIVersion: rbacv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	for _, mut := range mutations {
		mut(s)
	}
	return s
}

func WithRoleBindingSubjectServiceAccount(saNamespace, saName string) func(account *rbacv1.RoleBinding) {
	return func(cb *rbacv1.RoleBinding) {
		cb.Subjects = []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Namespace: saNamespace,
				Name:      saName,
			},
		}
	}
}

func WithRoleBindingClusterRoleRef(roleName string) func(account *rbacv1.RoleBinding) {
	return func(cb *rbacv1.RoleBinding) {
		cb.RoleRef = rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     roleName,
			APIGroup: "rbac.authorization.k8s.io",
		}
	}
}

func WithClusterRoleBindingSubjectServiceAccount(saNamespace, saName string) func(account *rbacv1.ClusterRoleBinding) {
	return func(cb *rbacv1.ClusterRoleBinding) {
		cb.Subjects = []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Namespace: saNamespace,
				Name:      saName,
			},
		}
	}
}

func WithClusterRoleBindingRoleRef(roleName string) func(account *rbacv1.ClusterRoleBinding) {
	return func(cb *rbacv1.ClusterRoleBinding) {
		cb.RoleRef = rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     roleName,
			APIGroup: "rbac.authorization.k8s.io",
		}
	}
}

func NewClusterRoleBinding(name string, mutations ...func(account *rbacv1.ClusterRoleBinding)) *rbacv1.ClusterRoleBinding {
	s := &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterRoleBinding",
			APIVersion: rbacv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	for _, mut := range mutations {
		mut(s)
	}
	return s
}

func NewConfigMapWithBinaryData(namespace string, name string, data []byte, options ...reconcilertesting.ConfigMapOption) runtime.Object {
	return reconcilertesting.NewConfigMap(
		name,
		namespace,
		append(options, func(configMap *corev1.ConfigMap) {
			if configMap.BinaryData == nil {
				configMap.BinaryData = make(map[string][]byte, 1)
			}
			if data == nil {
				data = []byte("")
			}
			configMap.BinaryData[base.ConfigMapDataKey] = data
		})...,
	)
}

func NewConfigMapWithTextData(namespace, name string, data map[string]string, options ...reconcilertesting.ConfigMapOption) runtime.Object {
	options = append(options,
		func(configMap *corev1.ConfigMap) {
			configMap.Data = data
		},
	)
	return reconcilertesting.NewConfigMap(
		name,
		namespace,
		options...,
	)
}

func NewConfigMapFromContract(contract *contract.Contract, namespace string, name string, format string, options ...reconcilertesting.ConfigMapOption) runtime.Object {
	var data []byte
	var err error
	if format == base.Protobuf {
		data, err = proto.Marshal(contract)
	} else {
		data, err = protojson.Marshal(contract)
	}
	if err != nil {
		panic(err)
	}

	return NewConfigMapWithBinaryData(namespace, name, data, options...)
}

func ConfigMapUpdate(namespace string, name string, format string, contract *contract.Contract, options ...reconcilertesting.ConfigMapOption) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "ConfigMap",
		},
		namespace,
		NewConfigMapFromContract(contract, namespace, name, format, options...),
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

func NewLegacySSLSecret(ns, name string) *corev1.Secret {

	ca, userKey, userCert := loadCerts()

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            name,
			ResourceVersion: SecretResourceVersion,
			UID:             SecretUUID,
		},
		Data: map[string][]byte{
			security.CaCertificateKey: ca,
			security.UserKey:          userKey,
			security.UserCertificate:  userCert,
		},
	}
}

func NewLegacySASLSSLSecret(ns, name string) *corev1.Secret {

	ca, userKey, userCert := loadCerts()

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            name,
			ResourceVersion: SecretResourceVersion,
			UID:             SecretUUID,
		},
		Data: map[string][]byte{
			security.CaCertificateKey: ca,
			security.UserKey:          userKey,
			security.UserCertificate:  userCert,
			"username":                []byte("user"),
			security.SaslPasswordKey:  []byte("password"),
			"saslType":                []byte("PLAIN"),
		},
	}
}

func NewLegacySASLSecret(ns, name string) *corev1.Secret {

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            name,
			ResourceVersion: SecretResourceVersion,
			UID:             SecretUUID,
		},
		Data: map[string][]byte{
			"username":               []byte("user"),
			security.SaslPasswordKey: []byte("password"),
			"saslType":               []byte("PLAIN"),
		},
	}
}

func NewSASLSSLSecret(ns, name string) *corev1.Secret {

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
			security.SaslUserKey:      []byte("user"),
			security.SaslPasswordKey:  []byte("password"),
			"type":                    []byte("PLAIN"),
		},
	}
}

func NewKedaSecret(ns, name string) *corev1.Secret {

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			OwnerReferences: []metav1.OwnerReference{
				ConsumerGroupAsOwnerRef(),
			},
		},
		StringData: map[string]string{
			"sasl": "plaintext",
			"tls":  "enable",
		},
	}
}

// ClusterRoleOption enables further configuration of a ClusterRole.
type ClusterRoleOption func(role *rbacv1.ClusterRole)

func NewClusterRole(name string, cro ...ClusterRoleOption) *rbacv1.ClusterRole {
	cr := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	for _, opt := range cro {
		opt(cr)
	}

	return cr
}

// WithClusterRoleRules is a ClusterRoleOption that adds the given rules to the ClusterRole.
func WithClusterRoleRules(rules ...rbacv1.PolicyRule) ClusterRoleOption {
	return func(role *rbacv1.ClusterRole) {
		role.Rules = append(role.Rules, rules...)
	}
}

// WithClusterRoleLabel is a ClusterRoleOption that adds the given labels to the ClusterRole.
func WithClusterRoleLabel(key, value string) ClusterRoleOption {
	return func(role *rbacv1.ClusterRole) {
		if role.Labels == nil {
			role.Labels = make(map[string]string)
		}
		role.Labels[key] = value
	}
}

func loadCerts() (ca, userKey, userCert []byte) {
	ca, err := os.ReadFile("testdata/ca.crt")
	if err != nil {
		panic(err)
	}

	userKey, err = os.ReadFile("testdata/user.key")
	if err != nil {
		panic(err)
	}

	userCert, err = os.ReadFile("testdata/user.crt")
	if err != nil {
		panic(err)
	}

	return ca, userKey, userCert
}

type KRShapedOption func(obj duckv1.KRShaped)

func WithDeletedTimeStamp(obj duckv1.KRShaped) {
	metaObj := obj.(metav1.Object)
	metaObj.SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
}

func StatusConfigParsed(obj duckv1.KRShaped) {
	obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrue(base.ConditionConfigParsed)
}

func StatusConfigNotParsed(reason string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(base.ConditionConfigParsed, reason, "")
	}
}

func StatusConfigMapUpdatedReady(env *config.Env) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrueWithReason(
			base.ConditionConfigMapUpdated,
			fmt.Sprintf("Config map %s updated", env.DataPlaneConfigMapAsString()),
			"",
		)
	}
}

func StatusConfigMapNotUpdatedReady(reason, message string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(
			base.ConditionConfigMapUpdated,
			reason,
			message,
		)
	}
}

func StatusTopicReadyWithName(topic string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrueWithReason(
			base.ConditionTopicReady,
			fmt.Sprintf("Topic %s created", topic),
			"",
		)
	}
}

func StatusTopicReadyWithOwner(topic, owner string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrueWithReason(
			base.ConditionTopicReady,
			fmt.Sprintf("Topic %s (owner %s)", topic, owner),
			"",
		)
	}
}

func StatusControllerOwnsTopic(topicOwner string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		allocateStatusAnnotations(obj)
		obj.GetStatus().Annotations[base.TopicOwnerAnnotation] = topicOwner
	}
}

func StatusTopicNotPresentErr(topic string, err error) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(
			base.ConditionTopicReady,
			base.ReasonTopicNotPresentOrInvalid,
			fmt.Sprintf("topics %v: "+SinkNotPresentErrFormat, []string{topic}, []string{topic}, err),
		)
	}
}

func StatusFailedToCreateTopic(topicName string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(
			base.ConditionTopicReady,
			fmt.Sprintf("Failed to create topic: %s", topicName),
			"%v",
			fmt.Errorf("failed to create topic"),
		)
	}
}

func StatusTopicNotPresentOrInvalid(topicName string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(
			base.ConditionTopicReady,
			base.ReasonTopicNotPresentOrInvalid,
			fmt.Sprintf("topics %v: invalid topic %s", []string{topicName}, topicName),
		)
	}
}

func StatusInitialOffsetsCommitted(obj duckv1.KRShaped) {
	obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrue(base.ConditionInitialOffsetsCommitted)
}

func StatusDataPlaneAvailable(obj duckv1.KRShaped) {
	obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrue(base.ConditionDataPlaneAvailable)
}

func StatusDataPlaneNotAvailable(obj duckv1.KRShaped) {
	obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(
		base.ConditionDataPlaneAvailable,
		base.ReasonDataPlaneNotAvailable,
		base.MessageDataPlaneNotAvailable,
	)
}

func StatusProbeSucceeded(obj duckv1.KRShaped) {
	obj.GetConditionSet().Manage(obj.GetStatus()).MarkTrue(base.ConditionProbeSucceeded)
}

func StatusProbeFailed(status prober.Status) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		obj.GetConditionSet().Manage(obj.GetStatus()).MarkFalse(
			base.ConditionProbeSucceeded,
			"ProbeStatus",
			fmt.Sprintf("status: %s", status.String()),
		)
	}
}

func allocateStatusAnnotations(obj duckv1.KRShaped) {
	if obj.GetStatus().Annotations == nil {
		obj.GetStatus().Annotations = make(map[string]string, 1)
	}
}

type PodOption func(pod *corev1.Pod)

func NewDispatcherPod(name string, options ...PodOption) *corev1.Pod {
	p := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: SystemNamespace,
			UID:       DispatcherPodUUID,
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: kafkaeventing.DispatcherVolumeName,
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: name,
						},
					},
				},
			}},
		},
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

func PodLabel(key, value string) PodOption {
	return func(pod *corev1.Pod) {
		if pod.Labels == nil {
			pod.Labels = make(map[string]string, 2)
		}
		pod.Labels[key] = value
	}
}

func PodPending() PodOption {
	return func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodPending
	}
}

func DispatcherPodAsOwnerReference(name string) reconcilertesting.ConfigMapOption {
	d := NewDispatcherPod(name)
	return func(configMap *corev1.ConfigMap) {
		configMap.OwnerReferences = append(configMap.OwnerReferences, metav1.OwnerReference{
			APIVersion:         d.APIVersion,
			Kind:               d.Kind,
			Name:               d.Name,
			UID:                d.UID,
			Controller:         pointer.Bool(true),
			BlockOwnerDeletion: pointer.Bool(true),
		})
	}
}

func ConfigmapOwnerReferenceBroker(uid types.UID, name string, controller bool) reconcilertesting.ConfigMapOption {
	return func(configMap *corev1.ConfigMap) {
		appendBrokerOwnerRef(&configMap.ObjectMeta, uid, name, controller)
	}
}

func ServiceAccountOwnerReferenceBroker(uid types.UID, name string, controller bool) func(account *corev1.ServiceAccount) {
	return func(account *corev1.ServiceAccount) {
		appendBrokerOwnerRef(&account.ObjectMeta, uid, name, controller)
	}
}

func RoleBindingOwnerReferenceBroker(uid types.UID, name string, controller bool) func(rb *rbacv1.RoleBinding) {
	return func(rb *rbacv1.RoleBinding) {
		appendBrokerOwnerRef(&rb.ObjectMeta, uid, name, controller)
	}
}

func DeploymentOwnerReferenceBroker(uid types.UID, name string, controller bool) func(d *appsv1.Deployment) {
	return func(d *appsv1.Deployment) {
		appendBrokerOwnerRef(&d.ObjectMeta, uid, name, controller)
	}
}

func ServiceOwnerReferenceBroker(uid types.UID, name string, controller bool) func(d *corev1.Service) {
	return func(d *corev1.Service) {
		appendBrokerOwnerRef(&d.ObjectMeta, uid, name, controller)
	}
}

func appendBrokerOwnerRef(obj *metav1.ObjectMeta, uid types.UID, name string, controller bool) {
	references := obj.GetOwnerReferences()
	obj.SetOwnerReferences(
		append(references, metav1.OwnerReference{
			APIVersion:         eventing.SchemeGroupVersion.String(),
			Kind:               "Broker",
			Name:               name,
			UID:                uid,
			Controller:         pointer.Bool(controller),
			BlockOwnerDeletion: pointer.Bool(true),
		}),
	)
}

func PodRunning() PodOption {
	return func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodRunning
	}
}

func PodAnnotations(annotations map[string]string) PodOption {
	return func(pod *corev1.Pod) {
		pod.Annotations = annotations
	}
}

func DataPlaneConfigMap(namespace, name, key string, options ...reconcilertesting.ConfigMapOption) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string]string{
			key: "",
		},
	}

	for _, opt := range options {
		opt(cm)
	}

	return cm
}

func DataPlaneConfigInitialOffset(key string, offset sources.Offset) reconcilertesting.ConfigMapOption {
	return func(cm *corev1.ConfigMap) {
		props := properties.MustLoadString(cm.Data[key])
		_, _, _ = props.Set("auto.offset.reset", string(offset))
		cm.Data[key] = props.String()
	}
}

func ReactorKEDAEnabled() clientgotesting.ReactionFunc {
	return func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
		if action.Matches("list", "scaledobjects") {
			return true, nil, nil
		}
		return false, nil, nil
	}
}
