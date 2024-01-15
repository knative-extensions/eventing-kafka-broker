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

package autoscaler

const (
	// Autoscaling refers to the autoscaling group.
	Autoscaling = "autoscaling.eventing.knative.dev"

	// AutoscalingClassAnnotation is the annotation for the explicit class of
	// scaler that a particular resource has opted into.
	AutoscalingClassAnnotation = Autoscaling + "/class"

	// AutoscalingClassDisabledAnnotationValue is the value of the AutoscalingClassAnnotation with
	// disabled autoscaling.
	AutoscalingClassDisabledAnnotationValue = "disabled"

	// AutoscalingMinScaleAnnotation is the annotation to specify the minimum number of replicas to scale down to.
	AutoscalingMinScaleAnnotation = Autoscaling + "/min-scale"
	// AutoscalingMaxScaleAnnotation is the annotation to specify the maximum number of replicas to scale out to.
	AutoscalingMaxScaleAnnotation = Autoscaling + "/max-scale"

	// AutoscalingPollingIntervalAnnotation is the annotation that refers to the interval in seconds the autoscaler
	// uses to poll metrics in order to inform its scaling decisions.
	AutoscalingPollingIntervalAnnotation = Autoscaling + "/polling-interval"

	// AutoscalingCooldownPeriodAnnotation is the annotation that refers to the period autoscaler waits until it
	// scales a ConsumerGroup down.
	AutoscalingCooldownPeriodAnnotation = Autoscaling + "/cooldown-period"

	// AutoscalingLagThreshold is the annotation that refers to the lag on the current
	// consumer group that's used for scaling (1<->N)
	AutoscalingLagThreshold = Autoscaling + "/lag-threshold"

	// AutoscalingActivationLagThreshold is the annotation that refers to the lag on the
	// current consumer group that's used for activation (0<->1)
	AutoscalingActivationLagThreshold = Autoscaling + "/activation-lag-threshold"

	// DefaultPollingInterval is the default value for AutoscalingPollingIntervalAnnotation.
	DefaultPollingInterval = 10
	// DefaultCooldownPeriod is the default value for AutoscalingCooldownPeriodAnnotation.
	DefaultCooldownPeriod = 30
	// DefaultMinReplicaCount is the default value for AutoscalingMinScaleAnnotation
	DefaultMinReplicaCount = 0
	// DefaultMaxReplicaCount is the default value for AutoscalingMaxScaleAnnotation.
	DefaultMaxReplicaCount = 50
	// DefaultLagThreshold is the default value for AutoscalingLagThreshold.
	DefaultLagThreshold = 100
)
