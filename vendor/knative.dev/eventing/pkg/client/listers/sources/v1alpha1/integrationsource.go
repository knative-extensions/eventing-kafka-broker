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

package v1alpha1

import (
	labels "k8s.io/apimachinery/pkg/labels"
	listers "k8s.io/client-go/listers"
	cache "k8s.io/client-go/tools/cache"
	sourcesv1alpha1 "knative.dev/eventing/pkg/apis/sources/v1alpha1"
)

// IntegrationSourceLister helps list IntegrationSources.
// All objects returned here must be treated as read-only.
type IntegrationSourceLister interface {
	// List lists all IntegrationSources in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*sourcesv1alpha1.IntegrationSource, err error)
	// IntegrationSources returns an object that can list and get IntegrationSources.
	IntegrationSources(namespace string) IntegrationSourceNamespaceLister
	IntegrationSourceListerExpansion
}

// integrationSourceLister implements the IntegrationSourceLister interface.
type integrationSourceLister struct {
	listers.ResourceIndexer[*sourcesv1alpha1.IntegrationSource]
}

// NewIntegrationSourceLister returns a new IntegrationSourceLister.
func NewIntegrationSourceLister(indexer cache.Indexer) IntegrationSourceLister {
	return &integrationSourceLister{listers.New[*sourcesv1alpha1.IntegrationSource](indexer, sourcesv1alpha1.Resource("integrationsource"))}
}

// IntegrationSources returns an object that can list and get IntegrationSources.
func (s *integrationSourceLister) IntegrationSources(namespace string) IntegrationSourceNamespaceLister {
	return integrationSourceNamespaceLister{listers.NewNamespaced[*sourcesv1alpha1.IntegrationSource](s.ResourceIndexer, namespace)}
}

// IntegrationSourceNamespaceLister helps list and get IntegrationSources.
// All objects returned here must be treated as read-only.
type IntegrationSourceNamespaceLister interface {
	// List lists all IntegrationSources in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*sourcesv1alpha1.IntegrationSource, err error)
	// Get retrieves the IntegrationSource from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*sourcesv1alpha1.IntegrationSource, error)
	IntegrationSourceNamespaceListerExpansion
}

// integrationSourceNamespaceLister implements the IntegrationSourceNamespaceLister
// interface.
type integrationSourceNamespaceLister struct {
	listers.ResourceIndexer[*sourcesv1alpha1.IntegrationSource]
}
