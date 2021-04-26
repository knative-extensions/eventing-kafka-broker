/*
Copyright 2021 The Knative Authors

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

package message

import (
	"encoding"
	"encoding/binary"
)

// AsyncCommand represents an interface of an asynchronous command containing an id.
// This type is used in order to automatically correlate commands and their results based on the id.
type AsyncCommand interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	SerializedId() []byte
}

// Int64CommandId creates a command id from int64. Useful to use values like Generation as command id
func Int64CommandId(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}
