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
	"testing"

	"github.com/stretchr/testify/assert"
	"knative.dev/pkg/apis"
)

func TestKafkaChannelStatusSetAddress(t *testing.T) {
	status := KafkaChannelStatus{}
	status.SetAddress(apis.HTTP("localhost"))

	addressable := status.GetCondition(ConditionAddressable).IsTrue()

	assert.True(t, addressable)

	status.SetAddress(nil)

	notAddressable := status.GetCondition(ConditionAddressable).IsFalse()

	assert.True(t, notAddressable)
}

func TestKafkaChannelGetConditionSet(t *testing.T) {

	cs := (&KafkaChannel{}).GetConditionSet()

	assert.Equal(t, conditionSet, cs)
}

func TestKafkaChannelStatusGetConditionSet(t *testing.T) {

	cs := (&KafkaChannelStatus{}).GetConditionSet()

	assert.Equal(t, conditionSet, cs)
}

func TestRegisterAlternateKafkaChannelConditionSet(t *testing.T) {

	cs := apis.NewLivingConditionSet(apis.ConditionReady, "hello")

	RegisterConditionSet(cs)

	kc := KafkaChannel{}

	assert.Equal(t, cs, kc.GetConditionSet())
	assert.Equal(t, cs, kc.Status.GetConditionSet())
}
