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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
)

type ContractMarshaller contract.Contract

func (m *ContractMarshaller) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("generation", m.Generation)
	return encoder.AddArray("resources", zapcore.ArrayMarshalerFunc(func(encoder zapcore.ArrayEncoder) error {

		for _, r := range m.Resources {
			if err := encoder.AppendObject((*resourceMarshaller)(r)); err != nil {
				return err
			}
		}
		return nil
	}))
}

type resourceMarshaller contract.Resource

func (b *resourceMarshaller) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("uid", b.Uid)
	if err := encoder.AddArray("topics", zapcore.ArrayMarshalerFunc(func(encoder zapcore.ArrayEncoder) error {
		for _, t := range b.Topics {
			encoder.AppendString(t)
		}
		return nil
	})); err != nil {
		return err
	}
	encoder.AddString("bootstrapServers", b.BootstrapServers)
	if b.Ingress != nil {
		if err := encoder.AddObject("ingress", (*ingressMarshaller)(b.Ingress)); err != nil {
			return err
		}
	}

	if b.EgressConfig != nil {
		if err := encoder.AddObject("egressConfig", (*egressConfigMarshaller)(b.EgressConfig)); err != nil {
			return err
		}
	}

	return encoder.AddArray("egresses", zapcore.ArrayMarshalerFunc(func(encoder zapcore.ArrayEncoder) error {
		for _, e := range b.Egresses {
			if err := encoder.AppendObject((*egressMarshaller)(e)); err != nil {
				return err
			}
		}
		return nil
	}))
}

type ingressMarshaller contract.Ingress

func (i *ingressMarshaller) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	switch it := i.IngressType.(type) {
	case *contract.Ingress_Path:
		encoder.AddString("ingress_path", it.Path)
	case *contract.Ingress_Host:
		encoder.AddString("ingress_host", it.Host)
	}

	encoder.AddString("contentMode", i.ContentMode.String())
	return nil
}

type egressMarshaller contract.Egress

func (e *egressMarshaller) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("uid", e.Uid)
	encoder.AddString("consumerGroup", e.ConsumerGroup)
	encoder.AddString("destination", e.Destination)

	switch rs := e.ReplyStrategy.(type) {
	case *contract.Egress_ReplyUrl:
		encoder.AddString("replyToUrl", rs.ReplyUrl)
	case *contract.Egress_ReplyToOriginalTopic:
		encoder.AddBool("replyToOriginalTopic", true)
	}

	if e.Filter != nil {
		return encoder.AddObject("filter", (*filterMarshaller)(e.Filter))
	}
	return nil
}

type filterMarshaller contract.Filter

func (f *filterMarshaller) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	return encoder.AddReflected("attributes", f.Attributes)
}

type egressConfigMarshaller contract.EgressConfig

func (e *egressConfigMarshaller) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("deadLetter", e.DeadLetter)
	return nil
}
