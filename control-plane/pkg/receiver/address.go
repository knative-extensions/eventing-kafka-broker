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
	"net/url"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

func Address(host string, object metav1.Object) *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   fmt.Sprintf("/%s/%s", object.GetNamespace(), object.GetName()),
	}
}

// HTTPAddress returns the addressable
func HTTPAddress(host string, audience *string, object metav1.Object) duckv1.Addressable {
	httpAddress := duckv1.Addressable{
		Name:     pointer.String("http"),
		URL:      apis.HTTP(host),
		Audience: audience,
	}
	httpAddress.URL.Path = fmt.Sprintf("/%s/%s", object.GetNamespace(), object.GetName())
	return httpAddress
}

// HTTPAddress returns the addressable
func ChannelHTTPAddress(host string, audience *string) duckv1.Addressable {
	httpAddress := duckv1.Addressable{
		Name:     pointer.String("http"),
		URL:      apis.HTTP(host),
		Audience: audience,
	}
	return httpAddress
}

func HTTPSAddress(host string, audience *string, object metav1.Object, caCerts *string) duckv1.Addressable {
	httpsAddress := duckv1.Addressable{
		Name:     pointer.String("https"),
		URL:      apis.HTTPS(host),
		CACerts:  caCerts,
		Audience: audience,
	}
	httpsAddress.URL.Path = fmt.Sprintf("/%s/%s", object.GetNamespace(), object.GetName())
	return httpsAddress
}
