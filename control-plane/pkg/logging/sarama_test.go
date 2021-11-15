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

package logging_test

import (
	"bytes"
	"io"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	. "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
)

func TestSaramaZapLogger(t *testing.T) {
	t.Run("Print", func(t *testing.T) {
		var logOutput bytes.Buffer
		logger := newSaramaLogger(&logOutput)

		logger.Print("test", 42)

		const expect = `{"level":"debug","msg":"test42"}` + "\n"

		if got := logOutput.String(); got != expect {
			t.Errorf("Expected\n  %sgot\n  %s", expect, got)
		}
	})

	t.Run("Println", func(t *testing.T) {
		var logOutput bytes.Buffer
		logger := newSaramaLogger(&logOutput)

		logger.Println("test", 42)

		const expect = `{"level":"debug","msg":"test42"}` + "\n"

		if got := logOutput.String(); got != expect {
			t.Errorf("Expected\n  %sgot\n  %s", expect, got)
		}
	})

	t.Run("Printf", func(t *testing.T) {
		var logOutput bytes.Buffer
		logger := newSaramaLogger(&logOutput)

		logger.Printf("test %d", 42)

		const expect = `{"level":"debug","msg":"test 42"}` + "\n"

		if got := logOutput.String(); got != expect {
			t.Errorf("Expected\n  %sgot\n  %s", expect, got)
		}
	})
}

// newSaramaLogger returns a SaramaZapLogger configured with a logger that
// writes to the given io.Writer using the JSON encoding.
func newSaramaLogger(logOutput io.Writer, logOptions ...zap.Option) *SaramaZapLogger {
	zapLogger := newZapLogger(logOutput, logOptions...)
	return NewSaramaLogger(zapLogger.Sugar())
}

// newZapLogger returns a zap logger with the same configuration returned by
// zap.NewExample(), except that it writes to the given io.Writer instead of
// os.Stdout.
func newZapLogger(w io.Writer, options ...zap.Option) *zap.Logger {
	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		NameKey:        "logger",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}

	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderCfg), zapcore.AddSync(w), zap.DebugLevel)

	return zap.New(core).WithOptions(options...)
}
