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

package main

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"net"
	"os"
	"sync"

	"go.uber.org/zap"
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/network"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	"knative.dev/control-protocol/test/test_images"
	"knative.dev/pkg/logging"
)

func main() {
	host := os.Getenv("HOST")

	devLogger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger := devLogger.Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	ctrlService, err := network.StartControlClient(ctx, &net.Dialer{}, host)
	if err != nil {
		panic(err)
	}

	acceptMessagePayload := generateRandomPayload()
	failMessagePayload := generateRandomPayload()

	logger.Info("Started control server")

	var doneWg sync.WaitGroup
	doneWg.Add(1)

	var receivedMessages []ctrl.ServiceMessage
	ctrlService.MessageHandler(ctrlservice.MessageRouter{
		test_images.AcceptMessage: ctrl.MessageHandlerFunc(func(ctx context.Context, message ctrl.ServiceMessage) {
			logger.Infof("Received accept message with payload length %d", len(message.Payload()))

			receivedMessages = append(receivedMessages, message)
			message.Ack()
		}),
		test_images.FailMessage: ctrl.MessageHandlerFunc(func(ctx context.Context, message ctrl.ServiceMessage) {
			logger.Infof("Received fail message with payload length %d", len(message.Payload()))

			receivedMessages = append(receivedMessages, message)
			message.AckWithError(errors.New(test_images.ErrorAck))
		}),
		test_images.DoneMessage: ctrl.MessageHandlerFunc(func(ctx context.Context, message ctrl.ServiceMessage) {
			logger.Infof("Received done message with payload length %d", len(message.Payload()))

			receivedMessages = append(receivedMessages, message)
			message.Ack()
			doneWg.Done()
		}),
	})

	err = ctrlService.SendAndWaitForAck(test_images.AcceptMessage, test_images.BinaryPayload(acceptMessagePayload))
	if err != nil {
		panic(err)
	}
	logger.Infof("Sent accept message with payload length %d", len(acceptMessagePayload))

	err = ctrlService.SendAndWaitForAck(test_images.FailMessage, test_images.BinaryPayload(failMessagePayload))
	if err == nil {
		panic("Expecting error")
	}
	logger.Infof("Sent fail message with payload length %d", len(failMessagePayload))

	err = ctrlService.SendAndWaitForAck(test_images.DoneMessage, nil)
	if err != nil {
		panic(err)
	}
	logger.Infof("Sent done message")

	// Wait for the done message
	doneWg.Wait()

	// Let's check the messages
	if len(receivedMessages) != 3 {
		logger.Fatalf("Expecting 3 received messages, actual: %d", len(receivedMessages))
	}

	receivedAcceptMessage := receivedMessages[0]
	if ctrl.OpCode(receivedAcceptMessage.Headers().OpCode()) != test_images.AcceptMessage {
		logger.Fatalf("Expecting accept opcode, actual: %d", receivedAcceptMessage.Headers().OpCode())
	}
	if !bytes.Equal(receivedAcceptMessage.Payload(), acceptMessagePayload) {
		logger.Fatalf(
			"Payload are not matching. Expected len: %d, actual len: %d. Expected value: %v, actual value: %v",
			len(acceptMessagePayload),
			len(receivedAcceptMessage.Payload()),
			acceptMessagePayload,
			receivedAcceptMessage.Payload(),
		)
	}

	receivedFailMessage := receivedMessages[1]
	if ctrl.OpCode(receivedFailMessage.Headers().OpCode()) != test_images.FailMessage {
		logger.Fatalf("Expecting failed opcode, actual: %d", receivedFailMessage.Headers().OpCode())
	}
	if !bytes.Equal(receivedFailMessage.Payload(), failMessagePayload) {
		logger.Fatalf(
			"Payload are not matching. Expected len: %d, actual len: %d. Expected value: %v, actual value: %v",
			len(failMessagePayload),
			len(receivedFailMessage.Payload()),
			failMessagePayload,
			receivedFailMessage.Payload(),
		)
	}

	receivedDoneMessage := receivedMessages[2]
	if ctrl.OpCode(receivedDoneMessage.Headers().OpCode()) != test_images.DoneMessage {
		logger.Fatalf("Expecting done opcode, actual: %d", receivedDoneMessage.Headers().OpCode())
	}
	if len(receivedDoneMessage.Payload()) != 0 {
		logger.Fatalf(
			"Payload are not matching. Expected len: 0, actual len: %d. Actual value: %v",
			len(receivedDoneMessage.Payload()),
			receivedDoneMessage.Payload(),
		)
	}

	// We're done!
	logger.Info("Test passed")
}

func generateRandomPayload() []byte {
	l := 1024
	l = l + rand.Intn(1024)

	b := make([]byte, l)
	rand.Read(b)

	return b
}
