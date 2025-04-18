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
	labels "k8s.io/apimachinery/pkg/labels"
	listers "k8s.io/client-go/listers"
	cache "k8s.io/client-go/tools/cache"
	kedav1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/apis/keda/v1alpha1"
)

// ClusterTriggerAuthenticationLister helps list ClusterTriggerAuthentications.
// All objects returned here must be treated as read-only.
type ClusterTriggerAuthenticationLister interface {
	// List lists all ClusterTriggerAuthentications in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*kedav1alpha1.ClusterTriggerAuthentication, err error)
	// Get retrieves the ClusterTriggerAuthentication from the index for a given name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*kedav1alpha1.ClusterTriggerAuthentication, error)
	ClusterTriggerAuthenticationListerExpansion
}

// clusterTriggerAuthenticationLister implements the ClusterTriggerAuthenticationLister interface.
type clusterTriggerAuthenticationLister struct {
	listers.ResourceIndexer[*kedav1alpha1.ClusterTriggerAuthentication]
}

// NewClusterTriggerAuthenticationLister returns a new ClusterTriggerAuthenticationLister.
func NewClusterTriggerAuthenticationLister(indexer cache.Indexer) ClusterTriggerAuthenticationLister {
	return &clusterTriggerAuthenticationLister{listers.New[*kedav1alpha1.ClusterTriggerAuthentication](indexer, kedav1alpha1.Resource("clustertriggerauthentication"))}
}
