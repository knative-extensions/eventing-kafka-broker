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

// Code generated by lister-gen. DO NOT EDIT.

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/listers"
	"k8s.io/client-go/tools/cache"
	v1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/apis/keda/v1alpha1"
)

// TriggerAuthenticationLister helps list TriggerAuthentications.
// All objects returned here must be treated as read-only.
type TriggerAuthenticationLister interface {
	// List lists all TriggerAuthentications in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.TriggerAuthentication, err error)
	// TriggerAuthentications returns an object that can list and get TriggerAuthentications.
	TriggerAuthentications(namespace string) TriggerAuthenticationNamespaceLister
	TriggerAuthenticationListerExpansion
}

// triggerAuthenticationLister implements the TriggerAuthenticationLister interface.
type triggerAuthenticationLister struct {
	listers.ResourceIndexer[*v1alpha1.TriggerAuthentication]
}

// NewTriggerAuthenticationLister returns a new TriggerAuthenticationLister.
func NewTriggerAuthenticationLister(indexer cache.Indexer) TriggerAuthenticationLister {
	return &triggerAuthenticationLister{listers.New[*v1alpha1.TriggerAuthentication](indexer, v1alpha1.Resource("triggerauthentication"))}
}

// TriggerAuthentications returns an object that can list and get TriggerAuthentications.
func (s *triggerAuthenticationLister) TriggerAuthentications(namespace string) TriggerAuthenticationNamespaceLister {
	return triggerAuthenticationNamespaceLister{listers.NewNamespaced[*v1alpha1.TriggerAuthentication](s.ResourceIndexer, namespace)}
}

// TriggerAuthenticationNamespaceLister helps list and get TriggerAuthentications.
// All objects returned here must be treated as read-only.
type TriggerAuthenticationNamespaceLister interface {
	// List lists all TriggerAuthentications in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.TriggerAuthentication, err error)
	// Get retrieves the TriggerAuthentication from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1alpha1.TriggerAuthentication, error)
	TriggerAuthenticationNamespaceListerExpansion
}

// triggerAuthenticationNamespaceLister implements the TriggerAuthenticationNamespaceLister
// interface.
type triggerAuthenticationNamespaceLister struct {
	listers.ResourceIndexer[*v1alpha1.TriggerAuthentication]
}
