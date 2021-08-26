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
	"knative.dev/pkg/apis"
)

const (

	// KafkaConditionScheduled is True when all KafkaSource consumers has been scheduled
	KafkaConditionScheduled apis.ConditionType = "Scheduled"
)

var (
	KafkaMTSourceCondSet = apis.NewLivingConditionSet(KafkaConditionSinkProvided, KafkaConditionScheduled, KafkaConditionInitialOffsetsCommitted, KafkaConditionConnectionEstablished)
)

func (s *KafkaSourceStatus) MarkScheduled() {
	KafkaSourceCondSet.Manage(s).MarkTrue(KafkaConditionScheduled)
}

func (s *KafkaSourceStatus) MarkNotScheduled(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(s).MarkFalse(KafkaConditionScheduled, reason, messageFormat, messageA...)
}
