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

package receiver

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPath(t *testing.T) {
	type args struct {
		namespace string
		name      string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "namespace/name",
			args: args{
				namespace: "broker-namespace",
				name:      "broker-name",
			},
			want: "/broker-namespace/broker-name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Path(tt.args.namespace, tt.args.name); got != tt.want {
				t.Errorf("Path() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPathFromObject(t *testing.T) {
	type args struct {
		obj metav1.Object
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "/namespace/name",
			args: args{
				obj: &metav1.ObjectMeta{
					Namespace: "my-namespace",
					Name:      "my-name",
				},
			},
			want: "/my-namespace/my-name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PathFromObject(tt.args.obj); got != tt.want {
				t.Errorf("PathFromObject() = %v, want %v", got, tt.want)
			}
		})
	}
}
