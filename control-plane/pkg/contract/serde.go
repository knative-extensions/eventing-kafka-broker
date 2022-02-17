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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type format string

const (
	Protobuf format = "protobuf"
	Json     format = "json"
)

type Serializer interface {
	Serialize(ct *Contract) ([]byte, error)
}

type Deserializer interface {
	Deserialize([]byte) (*Contract, error)
}

type FormatSerDe struct {
	Format format
}

func (s *FormatSerDe) Serialize(ct *Contract) ([]byte, error) {
	switch s.Format {
	case Protobuf:
		return proto.Marshal(ct)
	default:
		return protojson.Marshal(ct)
	}
}

func (s *FormatSerDe) Deserialize(data []byte) (*Contract, error) {
	ct := &Contract{}
	switch s.Format {
	case Protobuf:
		err := proto.Unmarshal(data, ct)
		return ct, err
	default:
		err := protojson.Unmarshal(data, ct)
		return ct, err
	}
}
