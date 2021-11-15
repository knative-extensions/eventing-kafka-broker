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

package logging

import (
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

// SaramaZapLogger is an implementation of the sarama.StdLogger interface that
// is backed by a zap logger.
type SaramaZapLogger struct {
	*zap.SugaredLogger
}

var _ sarama.StdLogger = (*SaramaZapLogger)(nil)

// NewSaramaZapLogger returns a SaramaZapLogger initialized with the given logger.
func NewSaramaLogger(l *zap.SugaredLogger) *SaramaZapLogger {
	return &SaramaZapLogger{SugaredLogger: l}
}

// Print implements sarama.StdLogger.
func (l *SaramaZapLogger) Print(v ...interface{}) {
	l.Debug(v...)
}

// Println implements sarama.StdLogger.
func (l *SaramaZapLogger) Println(v ...interface{}) {
	// (*zap.SugaredLogger).Debug uses println-style formatting, so
	// Print and Println are equivalent in this implementation.
	l.Debug(v...)
}

// Printf implements sarama.StdLogger.
func (l *SaramaZapLogger) Printf(format string, v ...interface{}) {
	l.Debugf(format, v...)
}
