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

package propagator

import (
	"bytes"
	"fmt"
	"text/template"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	configMapKey = "resources"
)

type Resources struct {
	Resources []unstructured.Unstructured
}

type TemplateData struct {
	Namespace string
}

func Unmarshal(cm *corev1.ConfigMap, templateData TemplateData) (Resources, error) {
	templateString, ok := cm.Data[configMapKey]
	if !ok {
		return Resources{}, nil
	}

	tmpl, err := template.New(fmt.Sprintf("%s/%s", cm.Namespace, cm.Name)).Parse(templateString)
	if err != nil {
		return Resources{}, fmt.Errorf("failed to unmarshal resources to propagate from ConfigMap %s/%s: %w", cm.GetNamespace(), cm.GetName(), err)
	}

	buf := bytes.Buffer{}
	if err := tmpl.Execute(&buf, templateData); err != nil {
		return Resources{}, fmt.Errorf("failed to unmarshal resources to propagate from ConfigMap %s/%s: %w", cm.GetNamespace(), cm.GetName(), err)
	}
	data := buf.String()

	resources := make([]map[string]interface{}, 0, 2)

	if err := yaml.Unmarshal([]byte(data), &resources); err != nil {
		return Resources{}, fmt.Errorf("failed to unmarshal resources to propagate from ConfigMap %s/%s: %w", cm.GetNamespace(), cm.GetName(), err)
	}

	ret := Resources{}
	for _, r := range resources {
		ret.Resources = append(ret.Resources, unstructured.Unstructured{Object: r})
	}

	return ret, nil
}

func UnmarshalTemplate(cm *corev1.ConfigMap) (Resources, error) {
	data, ok := cm.Data[configMapKey]
	if !ok {
		return Resources{}, nil
	}

	resources := make([]map[string]interface{}, 0, 2)

	if err := yaml.Unmarshal([]byte(data), &resources); err != nil {
		return Resources{}, fmt.Errorf("failed to unmarshal resources to propagate from ConfigMap %s/%s: %w", cm.GetNamespace(), cm.GetName(), err)
	}

	ret := Resources{}
	for _, r := range resources {
		ret.Resources = append(ret.Resources, unstructured.Unstructured{Object: r})
	}

	return ret, nil
}

func Marshal(resources Resources) (string, error) {

	res := make([]map[string]interface{}, 0, len(resources.Resources))
	for _, r := range resources.Resources {
		res = append(res, r.Object)
	}

	bytes, err := yaml.Marshal(res)
	if err != nil {
		return "", fmt.Errorf("failed to Marhsal additional resources: %w", err)
	}

	return string(bytes), nil
}
