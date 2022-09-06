/*
Copyright 2020 The Knative Authors

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

// Code generated by injection-gen. DO NOT EDIT.

package client

import (
	context "context"
	json "encoding/json"
	errors "errors"
	fmt "fmt"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	runtime "k8s.io/apimachinery/pkg/runtime"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	discovery "k8s.io/client-go/discovery"
	dynamic "k8s.io/client-go/dynamic"
	rest "k8s.io/client-go/rest"
	v1alpha1 "knative.dev/eventing-autoscaler-keda/third_party/pkg/apis/keda/v1alpha1"
	versioned "knative.dev/eventing-autoscaler-keda/third_party/pkg/client/clientset/versioned"
	typedkedav1alpha1 "knative.dev/eventing-autoscaler-keda/third_party/pkg/client/clientset/versioned/typed/keda/v1alpha1"
	injection "knative.dev/pkg/injection"
	dynamicclient "knative.dev/pkg/injection/clients/dynamicclient"
	logging "knative.dev/pkg/logging"
)

func init() {
	injection.Default.RegisterClient(withClientFromConfig)
	injection.Default.RegisterClientFetcher(func(ctx context.Context) interface{} {
		return Get(ctx)
	})
	injection.Dynamic.RegisterDynamicClient(withClientFromDynamic)
}

// Key is used as the key for associating information with a context.Context.
type Key struct{}

func withClientFromConfig(ctx context.Context, cfg *rest.Config) context.Context {
	return context.WithValue(ctx, Key{}, versioned.NewForConfigOrDie(cfg))
}

func withClientFromDynamic(ctx context.Context) context.Context {
	return context.WithValue(ctx, Key{}, &wrapClient{dyn: dynamicclient.Get(ctx)})
}

// Get extracts the versioned.Interface client from the context.
func Get(ctx context.Context) versioned.Interface {
	untyped := ctx.Value(Key{})
	if untyped == nil {
		if injection.GetConfig(ctx) == nil {
			logging.FromContext(ctx).Panic(
				"Unable to fetch knative.dev/eventing-autoscaler-keda/third_party/pkg/client/clientset/versioned.Interface from context. This context is not the application context (which is typically given to constructors via sharedmain).")
		} else {
			logging.FromContext(ctx).Panic(
				"Unable to fetch knative.dev/eventing-autoscaler-keda/third_party/pkg/client/clientset/versioned.Interface from context.")
		}
	}
	return untyped.(versioned.Interface)
}

type wrapClient struct {
	dyn dynamic.Interface
}

var _ versioned.Interface = (*wrapClient)(nil)

func (w *wrapClient) Discovery() discovery.DiscoveryInterface {
	panic("Discovery called on dynamic client!")
}

func convert(from interface{}, to runtime.Object) error {
	bs, err := json.Marshal(from)
	if err != nil {
		return fmt.Errorf("Marshal() = %w", err)
	}
	if err := json.Unmarshal(bs, to); err != nil {
		return fmt.Errorf("Unmarshal() = %w", err)
	}
	return nil
}

// KedaV1alpha1 retrieves the KedaV1alpha1Client
func (w *wrapClient) KedaV1alpha1() typedkedav1alpha1.KedaV1alpha1Interface {
	return &wrapKedaV1alpha1{
		dyn: w.dyn,
	}
}

type wrapKedaV1alpha1 struct {
	dyn dynamic.Interface
}

func (w *wrapKedaV1alpha1) RESTClient() rest.Interface {
	panic("RESTClient called on dynamic client!")
}

func (w *wrapKedaV1alpha1) ClusterTriggerAuthentications() typedkedav1alpha1.ClusterTriggerAuthenticationInterface {
	return &wrapKedaV1alpha1ClusterTriggerAuthenticationImpl{
		dyn: w.dyn.Resource(schema.GroupVersionResource{
			Group:    "keda",
			Version:  "v1alpha1",
			Resource: "clustertriggerauthentications",
		}),
	}
}

type wrapKedaV1alpha1ClusterTriggerAuthenticationImpl struct {
	dyn dynamic.NamespaceableResourceInterface
}

var _ typedkedav1alpha1.ClusterTriggerAuthenticationInterface = (*wrapKedaV1alpha1ClusterTriggerAuthenticationImpl)(nil)

func (w *wrapKedaV1alpha1ClusterTriggerAuthenticationImpl) Create(ctx context.Context, in *v1alpha1.ClusterTriggerAuthentication, opts v1.CreateOptions) (*v1alpha1.ClusterTriggerAuthentication, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "ClusterTriggerAuthentication",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Create(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ClusterTriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ClusterTriggerAuthenticationImpl) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return w.dyn.Delete(ctx, name, opts)
}

func (w *wrapKedaV1alpha1ClusterTriggerAuthenticationImpl) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	return w.dyn.DeleteCollection(ctx, opts, listOpts)
}

func (w *wrapKedaV1alpha1ClusterTriggerAuthenticationImpl) Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.ClusterTriggerAuthentication, error) {
	uo, err := w.dyn.Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ClusterTriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ClusterTriggerAuthenticationImpl) List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.ClusterTriggerAuthenticationList, error) {
	uo, err := w.dyn.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ClusterTriggerAuthenticationList{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ClusterTriggerAuthenticationImpl) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.ClusterTriggerAuthentication, err error) {
	uo, err := w.dyn.Patch(ctx, name, pt, data, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ClusterTriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ClusterTriggerAuthenticationImpl) Update(ctx context.Context, in *v1alpha1.ClusterTriggerAuthentication, opts v1.UpdateOptions) (*v1alpha1.ClusterTriggerAuthentication, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "ClusterTriggerAuthentication",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Update(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ClusterTriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ClusterTriggerAuthenticationImpl) UpdateStatus(ctx context.Context, in *v1alpha1.ClusterTriggerAuthentication, opts v1.UpdateOptions) (*v1alpha1.ClusterTriggerAuthentication, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "ClusterTriggerAuthentication",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.UpdateStatus(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ClusterTriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ClusterTriggerAuthenticationImpl) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("NYI: Watch")
}

func (w *wrapKedaV1alpha1) ScaledJobs(namespace string) typedkedav1alpha1.ScaledJobInterface {
	return &wrapKedaV1alpha1ScaledJobImpl{
		dyn: w.dyn.Resource(schema.GroupVersionResource{
			Group:    "keda",
			Version:  "v1alpha1",
			Resource: "scaledjobs",
		}),

		namespace: namespace,
	}
}

type wrapKedaV1alpha1ScaledJobImpl struct {
	dyn dynamic.NamespaceableResourceInterface

	namespace string
}

var _ typedkedav1alpha1.ScaledJobInterface = (*wrapKedaV1alpha1ScaledJobImpl)(nil)

func (w *wrapKedaV1alpha1ScaledJobImpl) Create(ctx context.Context, in *v1alpha1.ScaledJob, opts v1.CreateOptions) (*v1alpha1.ScaledJob, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "ScaledJob",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Create(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledJob{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledJobImpl) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return w.dyn.Namespace(w.namespace).Delete(ctx, name, opts)
}

func (w *wrapKedaV1alpha1ScaledJobImpl) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	return w.dyn.Namespace(w.namespace).DeleteCollection(ctx, opts, listOpts)
}

func (w *wrapKedaV1alpha1ScaledJobImpl) Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.ScaledJob, error) {
	uo, err := w.dyn.Namespace(w.namespace).Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledJob{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledJobImpl) List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.ScaledJobList, error) {
	uo, err := w.dyn.Namespace(w.namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledJobList{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledJobImpl) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.ScaledJob, err error) {
	uo, err := w.dyn.Namespace(w.namespace).Patch(ctx, name, pt, data, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledJob{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledJobImpl) Update(ctx context.Context, in *v1alpha1.ScaledJob, opts v1.UpdateOptions) (*v1alpha1.ScaledJob, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "ScaledJob",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Update(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledJob{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledJobImpl) UpdateStatus(ctx context.Context, in *v1alpha1.ScaledJob, opts v1.UpdateOptions) (*v1alpha1.ScaledJob, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "ScaledJob",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).UpdateStatus(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledJob{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledJobImpl) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("NYI: Watch")
}

func (w *wrapKedaV1alpha1) ScaledObjects(namespace string) typedkedav1alpha1.ScaledObjectInterface {
	return &wrapKedaV1alpha1ScaledObjectImpl{
		dyn: w.dyn.Resource(schema.GroupVersionResource{
			Group:    "keda",
			Version:  "v1alpha1",
			Resource: "scaledobjects",
		}),

		namespace: namespace,
	}
}

type wrapKedaV1alpha1ScaledObjectImpl struct {
	dyn dynamic.NamespaceableResourceInterface

	namespace string
}

var _ typedkedav1alpha1.ScaledObjectInterface = (*wrapKedaV1alpha1ScaledObjectImpl)(nil)

func (w *wrapKedaV1alpha1ScaledObjectImpl) Create(ctx context.Context, in *v1alpha1.ScaledObject, opts v1.CreateOptions) (*v1alpha1.ScaledObject, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "ScaledObject",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Create(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledObject{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledObjectImpl) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return w.dyn.Namespace(w.namespace).Delete(ctx, name, opts)
}

func (w *wrapKedaV1alpha1ScaledObjectImpl) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	return w.dyn.Namespace(w.namespace).DeleteCollection(ctx, opts, listOpts)
}

func (w *wrapKedaV1alpha1ScaledObjectImpl) Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.ScaledObject, error) {
	uo, err := w.dyn.Namespace(w.namespace).Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledObject{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledObjectImpl) List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.ScaledObjectList, error) {
	uo, err := w.dyn.Namespace(w.namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledObjectList{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledObjectImpl) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.ScaledObject, err error) {
	uo, err := w.dyn.Namespace(w.namespace).Patch(ctx, name, pt, data, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledObject{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledObjectImpl) Update(ctx context.Context, in *v1alpha1.ScaledObject, opts v1.UpdateOptions) (*v1alpha1.ScaledObject, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "ScaledObject",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Update(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledObject{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledObjectImpl) UpdateStatus(ctx context.Context, in *v1alpha1.ScaledObject, opts v1.UpdateOptions) (*v1alpha1.ScaledObject, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "ScaledObject",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).UpdateStatus(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ScaledObject{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1ScaledObjectImpl) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("NYI: Watch")
}

func (w *wrapKedaV1alpha1) TriggerAuthentications(namespace string) typedkedav1alpha1.TriggerAuthenticationInterface {
	return &wrapKedaV1alpha1TriggerAuthenticationImpl{
		dyn: w.dyn.Resource(schema.GroupVersionResource{
			Group:    "keda",
			Version:  "v1alpha1",
			Resource: "triggerauthentications",
		}),

		namespace: namespace,
	}
}

type wrapKedaV1alpha1TriggerAuthenticationImpl struct {
	dyn dynamic.NamespaceableResourceInterface

	namespace string
}

var _ typedkedav1alpha1.TriggerAuthenticationInterface = (*wrapKedaV1alpha1TriggerAuthenticationImpl)(nil)

func (w *wrapKedaV1alpha1TriggerAuthenticationImpl) Create(ctx context.Context, in *v1alpha1.TriggerAuthentication, opts v1.CreateOptions) (*v1alpha1.TriggerAuthentication, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "TriggerAuthentication",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Create(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.TriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1TriggerAuthenticationImpl) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return w.dyn.Namespace(w.namespace).Delete(ctx, name, opts)
}

func (w *wrapKedaV1alpha1TriggerAuthenticationImpl) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	return w.dyn.Namespace(w.namespace).DeleteCollection(ctx, opts, listOpts)
}

func (w *wrapKedaV1alpha1TriggerAuthenticationImpl) Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.TriggerAuthentication, error) {
	uo, err := w.dyn.Namespace(w.namespace).Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.TriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1TriggerAuthenticationImpl) List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.TriggerAuthenticationList, error) {
	uo, err := w.dyn.Namespace(w.namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.TriggerAuthenticationList{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1TriggerAuthenticationImpl) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.TriggerAuthentication, err error) {
	uo, err := w.dyn.Namespace(w.namespace).Patch(ctx, name, pt, data, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.TriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1TriggerAuthenticationImpl) Update(ctx context.Context, in *v1alpha1.TriggerAuthentication, opts v1.UpdateOptions) (*v1alpha1.TriggerAuthentication, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "TriggerAuthentication",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Update(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.TriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1TriggerAuthenticationImpl) UpdateStatus(ctx context.Context, in *v1alpha1.TriggerAuthentication, opts v1.UpdateOptions) (*v1alpha1.TriggerAuthentication, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda",
		Version: "v1alpha1",
		Kind:    "TriggerAuthentication",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).UpdateStatus(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.TriggerAuthentication{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKedaV1alpha1TriggerAuthenticationImpl) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("NYI: Watch")
}
