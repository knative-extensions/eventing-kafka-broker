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

package log

import (
	"go.uber.org/zap/zapcore"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
)

type BrokersMarshaller struct {
	Brokers *config.Brokers
}

func (m BrokersMarshaller) MarshalLogObject(encoder zapcore.ObjectEncoder) error {

	encoder.AddUint64("volumeGeneration", m.Brokers.VolumeGeneration)

	return encoder.AddArray("brokers", zapcore.ArrayMarshalerFunc(func(encoder zapcore.ArrayEncoder) error {

		for _, b := range m.Brokers.Brokers {
			if err := encoder.AppendObject(brokerMarshaller{broker: b}); err != nil {
				return err
			}
		}
		return nil
	}))
}

type brokerMarshaller struct {
	broker *config.Broker
}

func (b brokerMarshaller) MarshalLogObject(encoder zapcore.ObjectEncoder) error {

	encoder.AddString("id", b.broker.Id)
	encoder.AddString("topic", b.broker.Topic)
	encoder.AddString("deadLetterSink", b.broker.DeadLetterSink)

	return encoder.AddArray("triggers", zapcore.ArrayMarshalerFunc(func(encoder zapcore.ArrayEncoder) error {

		for _, t := range b.broker.Triggers {
			if err := encoder.AppendObject(triggerMarshaller{trigger: t}); err != nil {
				return err
			}
		}
		return nil
	}))
}

type triggerMarshaller struct {
	trigger *config.Trigger
}

func (m triggerMarshaller) MarshalLogObject(encoder zapcore.ObjectEncoder) error {

	encoder.AddString("id", m.trigger.Id)
	encoder.AddString("destination", m.trigger.Destination)
	return encoder.AddReflected("attributes", m.trigger.Attributes)
}
