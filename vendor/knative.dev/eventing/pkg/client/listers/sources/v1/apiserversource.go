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

// Code generated by lister-gen. DO NOT EDIT.

package v1

import (
	labels "k8s.io/apimachinery/pkg/labels"
	listers "k8s.io/client-go/listers"
	cache "k8s.io/client-go/tools/cache"
	sourcesv1 "knative.dev/eventing/pkg/apis/sources/v1"
)

// ApiServerSourceLister helps list ApiServerSources.
// All objects returned here must be treated as read-only.
type ApiServerSourceLister interface {
	// List lists all ApiServerSources in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*sourcesv1.ApiServerSource, err error)
	// ApiServerSources returns an object that can list and get ApiServerSources.
	ApiServerSources(namespace string) ApiServerSourceNamespaceLister
	ApiServerSourceListerExpansion
}

// apiServerSourceLister implements the ApiServerSourceLister interface.
type apiServerSourceLister struct {
	listers.ResourceIndexer[*sourcesv1.ApiServerSource]
}

// NewApiServerSourceLister returns a new ApiServerSourceLister.
func NewApiServerSourceLister(indexer cache.Indexer) ApiServerSourceLister {
	return &apiServerSourceLister{listers.New[*sourcesv1.ApiServerSource](indexer, sourcesv1.Resource("apiserversource"))}
}

// ApiServerSources returns an object that can list and get ApiServerSources.
func (s *apiServerSourceLister) ApiServerSources(namespace string) ApiServerSourceNamespaceLister {
	return apiServerSourceNamespaceLister{listers.NewNamespaced[*sourcesv1.ApiServerSource](s.ResourceIndexer, namespace)}
}

// ApiServerSourceNamespaceLister helps list and get ApiServerSources.
// All objects returned here must be treated as read-only.
type ApiServerSourceNamespaceLister interface {
	// List lists all ApiServerSources in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*sourcesv1.ApiServerSource, err error)
	// Get retrieves the ApiServerSource from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*sourcesv1.ApiServerSource, error)
	ApiServerSourceNamespaceListerExpansion
}

// apiServerSourceNamespaceLister implements the ApiServerSourceNamespaceLister
// interface.
type apiServerSourceNamespaceLister struct {
	listers.ResourceIndexer[*sourcesv1.ApiServerSource]
}
