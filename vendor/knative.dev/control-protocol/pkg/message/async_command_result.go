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

import "encoding/binary"

// AsyncCommandResult is a data structure representing an asynchronous command result.
// This can be used in use cases where the controller sends a command, which is acked as soon as is received by the data plane,
// then the data plane performs an eventually long operation, and when this operation is completed this result is sent back to the control plane.
// This can be used in conjunction with reconciler.AsyncCommandNotificationStore.
type AsyncCommandResult struct {
	// CommandId is the command id
	CommandId []byte
	// Error is the eventual error string. Empty if no error happened
	Error string
}

// IsFailed returns true if this result contains an error
func (k AsyncCommandResult) IsFailed() bool {
	return k.Error != ""
}

func (k AsyncCommandResult) MarshalBinary() (data []byte, err error) {
	bytesNumber := 4 + len(k.CommandId)
	if k.IsFailed() {
		bytesNumber = bytesNumber + 4 + len(k.Error)
	}
	b := make([]byte, bytesNumber)
	binary.BigEndian.PutUint32(b[0:4], uint32(len(k.CommandId)))
	copy(b[4:4+len(k.CommandId)], k.CommandId)
	if k.IsFailed() {
		binary.BigEndian.PutUint32(b[4+len(k.CommandId):4+len(k.CommandId)+4], uint32(len(k.Error)))
		copy(b[4+len(k.CommandId)+4:4+len(k.CommandId)+4+len(k.Error)], k.Error)
	}
	return b, nil
}

func (k *AsyncCommandResult) UnmarshalBinary(data []byte) error {
	commandLength := int(binary.BigEndian.Uint32(data[0:4]))
	k.CommandId = data[4 : 4+commandLength]
	if len(data) > 4+commandLength {
		errLength := int(binary.BigEndian.Uint32(data[4+commandLength : 4+commandLength+4]))
		k.Error = string(data[4+commandLength+4 : 4+commandLength+4+errLength])
	}
	return nil
}

func ParseAsyncCommandResult(data []byte) (interface{}, error) {
	var acr AsyncCommandResult
	if err := acr.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return acr, nil
}
