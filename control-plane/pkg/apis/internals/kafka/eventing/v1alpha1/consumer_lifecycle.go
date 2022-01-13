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
	"knative.dev/pkg/reconciler"
)

const (
	ConsumerConditionContract = "Contract"
	ConsumerConditionBind     = "Bind"
)

var (
	consumerConditionSet = apis.NewLivingConditionSet(
		ConsumerConditionContract,
		ConsumerConditionBind,
	)
)

func (c *Consumer) GetConditionSet() apis.ConditionSet {
	return consumerConditionSet
}

func (c *Consumer) MarkReconcileContractFailed(err error) reconciler.Event {
	err = fmt.Errorf("failed to reconcile contract: %w", err)
	c.GetConditionSet().Manage(c.GetStatus()).MarkFalse(ConsumerConditionContract, "ReconcileContract", err.Error())
	return err
}

func (c *Consumer) MarkReconcileContractSucceeded() {
	c.GetConditionSet().Manage(c.GetStatus()).MarkTrue(ConsumerConditionContract)
}

func (c *Consumer) MarkBindFailed(err error) reconciler.Event {
	err = fmt.Errorf("failed to bind resource to pod: %w", err)
	c.GetConditionSet().Manage(c.GetStatus()).MarkFalse(ConsumerConditionBind, "ConsumerBinding", err.Error())
	return err
}

func (c *Consumer) MarkBindInProgress() {
	c.GetConditionSet().Manage(c.GetStatus()).MarkFalse(ConsumerConditionBind, "BindInProgress", "")
}

func (c *Consumer) MarkBindSucceeded() {
	c.GetConditionSet().Manage(c.GetStatus()).MarkTrue(ConsumerConditionBind)
}
