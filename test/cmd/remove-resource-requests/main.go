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

package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

func main() {

	file := flag.String("f", "", "Input file path")
	outFile := flag.String("o", "", "Output file path")
	flag.Parse()

	if *file == "" {
		log.Fatalln("file path is empty")
	}
	if *outFile == "" {
		log.Fatalln("out-file path is empty")
	}

	r, err := os.ReadFile(*file)
	if err != nil {
		log.Fatalln(err)
	}
	w, err := os.Create(*outFile)
	if err != nil {
		log.Fatalln(err)
	}
	defer w.Close()

	decoder := yaml.NewDecoder(bytes.NewReader(r))
	outResources := make([]map[string]interface{}, 0, 10)
	for {
		var resource map[string]interface{}
		err := decoder.Decode(&resource)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln(err)
		}

		removeResourceRequestsMap(resource)
		outResources = append(outResources, resource)
	}

	encoder := yaml.NewEncoder(w)
	for r := range outResources {
		if len(outResources[r]) == 0 {
			continue
		}
		if err := encoder.Encode(outResources[r]); err != nil {
			log.Fatalln(err)
		}
	}
}

func removeResourceRequestsMap(r map[string]interface{}) {
	for k, v := range r {
		if k == "requests" {
			delete(r, "requests")
			delete(r, "limits") // When there are no requests, requests will be set to the limits
			log.Println("Requests found", v)
		}
		if v, ok := r[k].(map[string]interface{}); ok {
			removeResourceRequestsMap(v)
		}
		if v, ok := r[k].([]interface{}); ok {
			removeResourceRequestsArr(v)
		}
	}
}

func removeResourceRequestsArr(r []interface{}) {
	for i := range r {
		if v, ok := r[i].(map[string]interface{}); ok {
			removeResourceRequestsMap(v)
		}
		if v, ok := r[i].([]interface{}); ok {
			removeResourceRequestsArr(v)
		}
	}
}
