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

package contract

import (
	"go.uber.org/zap/zapcore"
)

func (x *Contract) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("generation", x.Generation)
	return encoder.AddArray("resources", zapcore.ArrayMarshalerFunc(func(encoder zapcore.ArrayEncoder) error {

		for _, r := range x.Resources {
			if err := encoder.AppendObject(r); err != nil {
				return err
			}
		}
		return nil
	}))
}

func (x *Resource) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("uid", x.Uid)
	if err := encoder.AddArray("topics", zapcore.ArrayMarshalerFunc(func(encoder zapcore.ArrayEncoder) error {
		for _, t := range x.Topics {
			encoder.AppendString(t)
		}
		return nil
	})); err != nil {
		return err
	}
	encoder.AddString("bootstrapServers", x.BootstrapServers)
	if x.Ingress != nil {
		if err := encoder.AddObject("ingress", x.Ingress); err != nil {
			return err
		}
	}

	if x.EgressConfig != nil {
		if err := encoder.AddObject("egressConfig", x.EgressConfig); err != nil {
			return err
		}
	}

	return encoder.AddArray("egresses", zapcore.ArrayMarshalerFunc(func(encoder zapcore.ArrayEncoder) error {
		for _, e := range x.Egresses {
			if err := encoder.AppendObject(e); err != nil {
				return err
			}
		}
		return nil
	}))
}

func (x *Ingress) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("ingress_path", x.Path)
	encoder.AddString("ingress_host", x.Host)
	encoder.AddString("contentMode", x.ContentMode.String())
	return nil
}

func (x *Egress) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("uid", x.Uid)
	encoder.AddString("consumerGroup", x.ConsumerGroup)
	encoder.AddString("destination", x.Destination)

	switch rs := x.ReplyStrategy.(type) {
	case *Egress_ReplyUrl:
		encoder.AddString("replyToUrl", rs.ReplyUrl)
	case *Egress_ReplyToOriginalTopic:
		encoder.AddBool("replyToOriginalTopic", true)
	}

	if x.Filter != nil {
		return encoder.AddObject("filter", x.Filter)
	}
	return nil
}

func (x *Filter) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	return encoder.AddReflected("attributes", x.Attributes)
}

func (x *EgressConfig) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("deadLetter", x.DeadLetter)
	return nil
}
