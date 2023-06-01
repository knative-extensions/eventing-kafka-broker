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
	"k8s.io/utils/pointer"
	"knative.dev/pkg/apis"
	pkgduckv1 "knative.dev/pkg/apis/duck/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Address(host string, object metav1.Object) *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   fmt.Sprintf("/%s/%s", object.GetNamespace(), object.GetName()),
	}
}

// func AddressTLS(host string, object metav1.Object, caCerts string) *url.URL {
// 	return &url.URL{
// 		Scheme: "https",
// 		Host:   host,
// 		Path:   fmt.Sprintf("/%s/%s", object.GetNamespace(), object.GetName()),
// 		caCerts: caCerts,
// 	}
// }


// HTTPAddress returns the addressable
func HTTPAddress(host string, object metav1.Object) pkgduckv1.Addressable {
	httpAddress := pkgduckv1.Addressable{
		Name: pointer.String("http"),
		URL:  apis.HTTP(host),
	}
	httpAddress.URL.Path = fmt.Sprintf("/%s/%s", object.GetNamespace(), object.GetName())
	return httpAddress
}

func HTTPSAddress(host string, object metav1.Object, caCerts string) pkgduckv1.Addressable {
	httpsAddress := pkgduckv1.Addressable{
		Name:    pointer.String("https"),
		URL:     apis.HTTPS(host),
		CACerts: pointer.String(caCerts),
	}
	httpsAddress.URL.Path = fmt.Sprintf("/%s/%s", object.GetNamespace(), object.GetName())
	return httpsAddress
}