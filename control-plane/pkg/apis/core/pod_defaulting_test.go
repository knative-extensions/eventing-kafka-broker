/*
 * Copyright 2022 The Knative Authors
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

package core

import (
	"context"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/system"
)

func TestPodDefaulting(t *testing.T) {
	os.Setenv("SYSTEM_NAMESPACE", "knative-eventing")

	id := uuid.New()

	tests := []struct {
		name    string
		ctx     context.Context
		given   corev1.Pod
		want    corev1.Pod
		wantErr bool
	}{
		{
			name: "add volume source",
			ctx:  context.Background(),
			given: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-source-dispatcher",
					Namespace: system.Namespace(),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
				},
			},
			want: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-source-dispatcher",
					Namespace: system.Namespace(),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher"},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "add volume source, preserving another volume",
			ctx:  context.Background(),
			given: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-source-dispatcher",
					Namespace: system.Namespace(),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
					},
				},
			},
			want: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-source-dispatcher",
					Namespace: system.Namespace(),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
						{
							Name: "contract-resources",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher"},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "replace volume source, preserving another volume",
			ctx:  context.Background(),
			given: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-source-dispatcher",
					Namespace: system.Namespace(),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
						{
							Name: "contract-resources",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: id.String() + "-aaa"},
								},
							},
						},
					},
				},
			},
			want: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-source-dispatcher",
					Namespace: system.Namespace(),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
						{
							Name: "contract-resources",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher"},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "no changes to volume source, preserving another volume",
			ctx:  context.Background(),
			given: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-source-dispatcher",
					Namespace: system.Namespace(),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
						{
							Name: "contract-resources",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher"},
								},
							},
						},
					},
				},
			},
			want: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-source-dispatcher",
					Namespace: system.Namespace(),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
						{
							Name: "contract-resources",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher"},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "no namespace, no changes",
			ctx:  context.Background(),
			given: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "kafka-source-dispatcher"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
						{
							Name: "contract-resources",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher"},
								},
							},
						},
					},
				},
			},
			want: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "kafka-source-dispatcher"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
						{
							Name: "contract-resources",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher"},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "No name, use generator",
			ctx:  context.Background(),
			given: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: system.Namespace()},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
					},
				},
			},
			want: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: system.Namespace()},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "dispatcher",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "contract-resources",
									ReadOnly:  false,
									MountPath: "/etc/contract/contract-resources",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "contract-resources-2",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-source-dispatcher-2"},
								},
							},
						},
						{
							Name: "contract-resources",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: id.String()},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	var generator generatorFunc = func(*unstructured.Unstructured) uuid.UUID { return id }

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			given, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&tt.given)
			require.Nil(t, err)

			unstr := &unstructured.Unstructured{Object: given}
			if err := podDefaultingCallback(generator)(tt.ctx, unstr); (err != nil) != tt.wantErr {
				t.Errorf("podDefaultingCallback() error = %v, wantErr %v", err, tt.wantErr)
			}

			got := corev1.Pod{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstr.UnstructuredContent(), &got)
			require.Nil(t, err)

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Error("(-want,+got)", diff)
			}
		})
	}
}
