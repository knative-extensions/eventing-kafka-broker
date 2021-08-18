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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PathFromObject returns an HTTP request path given a generic object.
func PathFromObject(obj metav1.Object) string {
	return Path(obj.GetNamespace(), obj.GetName())
}

// Path returns an HTTP request path given namespace and name of an object.
func Path(namespace, name string) string {
	return fmt.Sprintf("/%s/%s", namespace, name)
}
