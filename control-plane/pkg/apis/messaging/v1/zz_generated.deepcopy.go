//go:build !ignore_autogenerated
// +build !ignore_autogenerated

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

// Code generated by deepcopy-gen. DO NOT EDIT.

package v1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *KafkaChannel) DeepCopyInto(out *KafkaChannel) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new KafkaChannel.
func (in *KafkaChannel) DeepCopy() *KafkaChannel {
	if in == nil {
		return nil
	}
	out := new(KafkaChannel)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *KafkaChannel) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *KafkaChannelList) DeepCopyInto(out *KafkaChannelList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]KafkaChannel, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new KafkaChannelList.
func (in *KafkaChannelList) DeepCopy() *KafkaChannelList {
	if in == nil {
		return nil
	}
	out := new(KafkaChannelList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *KafkaChannelList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *KafkaChannelSpec) DeepCopyInto(out *KafkaChannelSpec) {
	*out = *in
	in.ChannelableSpec.DeepCopyInto(&out.ChannelableSpec)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new KafkaChannelSpec.
func (in *KafkaChannelSpec) DeepCopy() *KafkaChannelSpec {
	if in == nil {
		return nil
	}
	out := new(KafkaChannelSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *KafkaChannelStatus) DeepCopyInto(out *KafkaChannelStatus) {
	*out = *in
	in.ChannelableStatus.DeepCopyInto(&out.ChannelableStatus)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new KafkaChannelStatus.
func (in *KafkaChannelStatus) DeepCopy() *KafkaChannelStatus {
	if in == nil {
		return nil
	}
	out := new(KafkaChannelStatus)
	in.DeepCopyInto(out)
	return out
}
