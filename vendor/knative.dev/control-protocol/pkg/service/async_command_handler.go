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

package service

import (
	"context"
	"encoding"
	"fmt"
	"reflect"

	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	control "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/message"
)

// AsyncCommandMessage is a command message that notifies asynchronously the completion of the execution.
// Look at NewAsyncCommandHandler for more details.
type AsyncCommandMessage struct {
	serviceMessage control.ServiceMessage

	parsedCommand message.AsyncCommand

	service      control.Service
	resultOpCode control.OpCode

	ctx context.Context
}

// Headers returns the message headers
func (ac *AsyncCommandMessage) Headers() control.MessageHeader {
	return ac.serviceMessage.Headers()
}

// ParsedCommand returns the command parsed in the specified type
func (ac *AsyncCommandMessage) ParsedCommand() message.AsyncCommand {
	return ac.parsedCommand
}

// NotifyFailed will notify a failure in the handling of the control message
// If err == nil, this acts the same as NotifySuccess
func (ac *AsyncCommandMessage) NotifyFailed(err error) {
	if err == nil {
		ac.NotifySuccess()
		return
	}

	commandResult := message.AsyncCommandResult{
		CommandId: ac.parsedCommand.SerializedId(),
		Error:     err.Error(),
	}

	if err := ac.service.SendAndWaitForAck(ac.resultOpCode, commandResult); err != nil {
		logging.FromContext(ac.ctx).Errorw("Error while propagating the async command result", zap.Any("asyncCommandResult", commandResult), zap.Error(err))
	}
}

// NotifySuccess will notify the success of the handling of the control message
func (ac *AsyncCommandMessage) NotifySuccess() {
	commandResult := message.AsyncCommandResult{
		CommandId: ac.parsedCommand.SerializedId(),
	}

	if err := ac.service.SendAndWaitForAck(ac.resultOpCode, commandResult); err != nil {
		logging.FromContext(ac.ctx).Errorw("Error while propagating the async command result", zap.Any("asyncCommandResult", commandResult), zap.Error(err))
	}
}

type asyncCommandHandler struct {
	svc          control.Service
	resultOpCode control.OpCode

	payloadType reflect.Type

	handler func(context.Context, AsyncCommandMessage)
}

// NewAsyncCommandHandler returns a control.MessageHandler that wraps the provided handler, but passing an AsyncCommandMessage.
// This handler automatically parses the message payload using the provided type, and using the AsyncCommandMessage it notifies the result of the command to the sender.
func NewAsyncCommandHandler(svc control.Service, payloadType message.AsyncCommand, resultOpCode control.OpCode, handler func(context.Context, AsyncCommandMessage)) control.MessageHandler {
	return &asyncCommandHandler{
		svc:          svc,
		resultOpCode: resultOpCode,
		payloadType:  reflect.Indirect(reflect.ValueOf(payloadType)).Type(),
		handler:      handler,
	}
}

// HandleServiceMessage implements control.MessageHandler
func (f *asyncCommandHandler) HandleServiceMessage(ctx context.Context, msg control.ServiceMessage) {
	// This creates a new instance of f.payloadType,
	// payloadValue contains the pointer to such instance
	payloadValue := reflect.New(f.payloadType).Interface()

	// This assertion is always true b/c payloadType is per contract a message.AsyncCommand,
	// which implements encoding.BinaryUnmarshaler
	err := payloadValue.(encoding.BinaryUnmarshaler).UnmarshalBinary(msg.Payload())
	if err != nil {
		logging.FromContext(ctx).Warnw("Error while parsing the async command", zap.Error(err))
		msg.AckWithError(fmt.Errorf("error while parsing the async command: %w", err))
		return
	}

	asyncCommandMsg := AsyncCommandMessage{
		serviceMessage: msg,
		parsedCommand:  payloadValue.(message.AsyncCommand),
		service:        f.svc,
		resultOpCode:   f.resultOpCode,
		ctx:            ctx,
	}

	// Message received, starting processing
	msg.Ack()

	f.handler(ctx, asyncCommandMsg)
}
