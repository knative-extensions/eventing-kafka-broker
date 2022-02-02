/*
 * Copyright 2021 The Knative Authors
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

package v1alpha1

import (
	"fmt"

	"knative.dev/pkg/apis"
)

const (
	ConditionConsumerGroupConsumers          apis.ConditionType = "Consumers"
	ConditionConsumerGroupConsumersScheduled apis.ConditionType = "ConsumersScheduled"
	// Labels
	KafkaChannelNameLabel = "kafkachannel-name"
)

var (
	conditionSet = apis.NewLivingConditionSet(
		ConditionConsumerGroupConsumers,
		ConditionConsumerGroupConsumersScheduled,
	)
)

func (c *ConsumerGroup) GetConditionSet() apis.ConditionSet {
	return conditionSet
}

func (cg *ConsumerGroup) MarkReconcileConsumersFailed(reason string, err error) error {
	err = fmt.Errorf("failed to reconcile consumers: %w", err)
	cg.GetConditionSet().Manage(cg.GetStatus()).MarkFalse(ConditionConsumerGroupConsumers, reason, err.Error())
	return err
}

func (cg *ConsumerGroup) MarkReconcileConsumersSucceeded() {
	cg.GetConditionSet().Manage(cg.GetStatus()).MarkTrue(ConditionConsumerGroupConsumers)
}

func (cg *ConsumerGroup) MarkScheduleConsumerFailed(reason string, err error) error {
	err = fmt.Errorf("failed to schedule consumers: %w", err)
	cg.GetConditionSet().Manage(cg.GetStatus()).MarkFalse(ConditionConsumerGroupConsumers, reason, err.Error())
	return err
}

func (cg *ConsumerGroup) MarkScheduleSucceeded() {
	cg.GetConditionSet().Manage(cg.GetStatus()).MarkTrue(ConditionConsumerGroupConsumersScheduled)
}
