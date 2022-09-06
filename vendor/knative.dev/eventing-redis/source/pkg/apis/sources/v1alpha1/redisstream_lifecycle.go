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

package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/pkg/apis"
)

const (
	// RedisStreamConditionReady has status True when the RedisStreamSource is ready to send events.
	RedisStreamConditionReady = apis.ConditionReady

	// RedisStreamConditionSinkProvided has status True when the RedisStreamSource has been configured with a sink target.
	RedisStreamConditionSinkProvided apis.ConditionType = "SinkProvided"

	// RedisStreamConditionDeployed has status True when the RedisStreamSource has had it's statefulset created.
	RedisStreamConditionDeployed apis.ConditionType = "Deployed"
)

var redisStreamCondSet = apis.NewLivingConditionSet(
	RedisStreamConditionSinkProvided,
	RedisStreamConditionDeployed,
)

// GetConditionSet retrieves the condition set for this resource. Implements the KRShaped interface.
func (*RedisStreamSource) GetConditionSet() apis.ConditionSet {
	return redisStreamCondSet
}

// GetGroupVersionKind returns the GroupVersionKind.
func (s *RedisStreamSource) GetGroupVersionKind() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("RedisStreamSource")
}

// GetUntypedSpec returns the spec of the RedisStreamSource.
func (s *RedisStreamSource) GetUntypedSpec() interface{} {
	return s.Spec
}

// GetCondition returns the condition currently associated with the given type, or nil.
func (s *RedisStreamSourceStatus) GetCondition(t apis.ConditionType) *apis.Condition {
	return redisStreamCondSet.Manage(s).GetCondition(t)
}

// GetTopLevelCondition returns the top level condition.
func (s *RedisStreamSourceStatus) GetTopLevelCondition() *apis.Condition {
	return redisStreamCondSet.Manage(s).GetTopLevelCondition()
}

// InitializeConditions sets relevant unset conditions to Unknown state.
func (s *RedisStreamSourceStatus) InitializeConditions() {
	redisStreamCondSet.Manage(s).InitializeConditions()
}

// MarkSink sets the condition that the source has a sink configured.
func (s *RedisStreamSourceStatus) MarkSink(uri string) {
	s.SinkURI = nil
	if len(uri) > 0 {
		if u, err := apis.ParseURL(uri); err != nil {
			redisStreamCondSet.Manage(s).MarkFalse(RedisStreamConditionSinkProvided, "SinkInvalid", "Failed to parse sink: %v", err)
		} else {
			s.SinkURI = u
			redisStreamCondSet.Manage(s).MarkTrue(RedisStreamConditionSinkProvided)
		}

	} else {
		redisStreamCondSet.Manage(s).MarkFalse(RedisStreamConditionSinkProvided, "SinkEmpty", "Sink has resolved to empty.")
	}
}

// MarkNoSink sets the condition that the source does not have a sink configured.
func (s *RedisStreamSourceStatus) MarkNoSink(reason, messageFormat string, messageA ...interface{}) {
	redisStreamCondSet.Manage(s).MarkFalse(RedisStreamConditionSinkProvided, reason, messageFormat, messageA...)
}

// PropagateStatefulSetAvailability uses the availability of the provided StatefulSet to determine if
// RedisStreamConditionDeployed should be marked as true or false.
func (s *RedisStreamSourceStatus) PropagateStatefulSetAvailability(d *appsv1.StatefulSet) {
	if d.Status.ReadyReplicas == *d.Spec.Replicas {
		redisStreamCondSet.Manage(s).MarkTrue(RedisStreamConditionDeployed)

		// Propagate the number of consumers
		s.Consumers = d.Status.Replicas
	} else {
		redisStreamCondSet.Manage(s).MarkUnknown(RedisStreamConditionDeployed, "StatefulSetUnavailable", "The StatefulSet '%s' is unavailable.", d.Name)
	}
}

// IsReady returns true if the resource is ready overall.
func (s *RedisStreamSourceStatus) IsReady() bool {
	return redisStreamCondSet.Manage(s).IsHappy()
}

// MarkNoRoleBinding sets the annotation that the source does not have a role binding
func (s *RedisStreamSourceStatus) MarkNoRoleBinding(reason string) {
	s.setAnnotation("roleBinding", reason)
}

// MarkRoleBinding sets the annotation that the source has a role binding
func (s *RedisStreamSourceStatus) MarkRoleBinding() {
	s.clearAnnotation("roleBinding")
}

// MarkNoServiceAccount sets the annotation that the source does not have a service account
func (s *RedisStreamSourceStatus) MarkNoServiceAccount(reason string) {
	s.setAnnotation("serviceAccount", reason)
}

// MarkRoleBinding sets the annotation that the source has a service account
func (s *RedisStreamSourceStatus) MarkServiceAccount() {
	s.clearAnnotation("serviceAccount")
}

func (s *RedisStreamSourceStatus) setAnnotation(name, value string) {
	if s.Annotations == nil {
		s.Annotations = make(map[string]string)
	}
	s.Annotations[name] = value
}

func (s *RedisStreamSourceStatus) clearAnnotation(name string) {
	delete(s.Annotations, name)
}
