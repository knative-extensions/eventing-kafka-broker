/*
 * Copyright 2024 The Knative Authors
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

package keda

import (
	"strings"
	"testing"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler/keda/mocks"
)

func TestGenerateScaledObjectName(t *testing.T) {
	type args struct {
		obj v1.Object
	}
	tests := []struct {
		name       string
		args       args
		wantPrefix string
		wantSuffix types.UID
	}{
		{
			name: "create scaled object name successfully",
			args: args{
				obj: mocks.ObjectMock{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GenerateScaledObjectName(tt.args.obj)
			soNameLength := len(got)
			if soNameLength != 39 {
				t.Errorf("GenerateScaledObjectName() length = %v, want length %v", len(got), soNameLength)
			}
			if !strings.HasPrefix(got, scaledObjectPrefixName) {
				t.Errorf("GenerateScaledObjectName() prefix = %v, want prefix %v", got, tt.wantPrefix)
			}
			uid := types.UID(strings.TrimLeft(got, scaledObjectPrefixName+"-"))
			uidLength := len(uid)
			if uidLength != 36 {
				t.Errorf("GenerateScaledObjectName() UID length = %v, want length 36", len(string(uid)))
			}
		})
	}
}
