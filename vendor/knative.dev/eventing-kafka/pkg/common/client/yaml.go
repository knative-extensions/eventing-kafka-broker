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

package client

import (
	"crypto/x509"
	"fmt"
	"regexp"

	"github.com/Shopify/sarama"
	"sigs.k8s.io/yaml"
)

// Regular Expression To Find All Certificates In Net.TLS.Config.RootPEMs Field
var regexRootPEMs = regexp.MustCompile(`(?s)\s*RootPEMs:.*-----END CERTIFICATE-----`)

//
// Extract (Parse & Remove) Top Level Kafka Version From Specified Sarama Confirm YAML String
//
// The Sarama.Config struct contains a top-level 'Version' field of type sarama.KafkaVersion.
// This type contains a package scoped [4]uint field called 'version' which we cannot unmarshal
// the yaml into.  Therefore, we instead support the user providing a 'Version' string of the
// format "major.minor.patch" (e.g. "2.3.0") which we will "extract" out of the YAML string
// and return as an actual sarama.KafkaVersion.
//
// In the case where the user has NOT specified, nil will be returned.
//
func extractKafkaVersion(saramaConfigYamlString string) (string, *sarama.KafkaVersion, error) {

	// Define Inline Struct To Marshall The Top Level Sarama Config "Kafka" Version Into
	type saramaConfigShell struct {
		Version string
	}

	// Unmarshal The Sarama Config Into The Shell
	shell := &saramaConfigShell{}
	err := yaml.Unmarshal([]byte(saramaConfigYamlString), shell)
	if err != nil {
		return saramaConfigYamlString, nil, err
	}

	// Return nil If Not Specified
	if len(shell.Version) <= 0 {
		return saramaConfigYamlString, nil, nil
	}

	// Attempt To Parse The Version String Into A Sarama.KafkaVersion
	kafkaVersion, err := sarama.ParseKafkaVersion(shell.Version)
	if err != nil {
		return saramaConfigYamlString, nil, err
	}

	// Remove The Version From The Sarama Config YAML String
	regex, err := regexp.Compile("\\s*Version:\\s*" + shell.Version + "\\s*")
	if err != nil {
		return saramaConfigYamlString, nil, err
	} else {
		updatedSaramaConfigYamlBytes := regex.ReplaceAll([]byte(saramaConfigYamlString), []byte("\n"))
		return string(updatedSaramaConfigYamlBytes), &kafkaVersion, nil
	}
}

/* Extract (Parse & Remove) TLS.Config Level RootPEMs From Specified Sarama Confirm YAML String

The Sarama.Config struct contains Net.TLS.Config which is a *tls.Config which cannot be parsed.
due to it being from another package and containing lots of func()s.  We do need the ability
however to provide Root Certificates in order to avoid having to disable verification via the
InsecureSkipVerify field.  Therefore, we support a custom 'RootPEMs' field which is an array
of strings containing the PEM file content.  This function will "extract" that content out
of the YAML string and return as a populated *x509.CertPool which can be assigned to the
Sarama.Config.Net.TLS.Config.RootCAs field.  In the case where the user has NOT specified
any PEM files we will return nil.

In order for the PEM file content to pass cleanly from the YAML string, which is itself
already a YAML string inside the K8S ConfigMap, it must be formatted and spaced correctly.
The following shows an example of the expected usage...

apiVersion: v1
kind: ConfigMap
metadata:
  name: config-kafka
  namespace: knative-eventing
data:
  sarama: |
    Admin:
      Timeout: 10000000000
    Net:
      KeepAlive: 30000000000
      TLS:
        Enable: true
        Config:
          RootCaPems:
          - |-
            -----BEGIN CERTIFICATE-----
            MIIGBDCCA+ygAwIBAgIJAKi1aEV58cQ1MA0GCSqGSIb3DQEBCwUAMIGOMQswCQYD
            ...
            2wk9rLRZaQnhspt6MhlmU0qkaEZpYND3emR2XZ07m51jXqDUgTjXYCSggImUsARs
            NAehp9bMeco=
            -----END CERTIFICATE-----
      SASL:
        Enable: true

...where you should make sure to use the YAML string syntax of "|-" in order to
prevent trailing linefeed. The indentation of the PEM content is also important
and must be aligned as shown.
*/
func extractRootCerts(saramaConfigYamlString string) (string, *x509.CertPool, error) {

	// Define Inline Struct To Marshall The TLS Config 'RootPEMs' Into
	type tlsConfigShell struct {
		Net struct {
			TLS struct {
				Config struct {
					RootPEMs []string
				}
			}
		}
	}

	// Unmarshal The TLS Config Into The Shell
	shell := &tlsConfigShell{}
	err := yaml.Unmarshal([]byte(saramaConfigYamlString), shell)
	if err != nil {
		return saramaConfigYamlString, nil, err
	}

	// Convenience Variable For The RootPEMs
	rootPEMs := shell.Net.TLS.Config.RootPEMs

	// Exit Early If No RootCert PEMs
	if rootPEMs == nil || len(rootPEMs) < 1 {
		return saramaConfigYamlString, nil, nil
	}

	// Create A New CertPool To Contain The Root PEMs
	certPool := x509.NewCertPool()

	// Populate The CertPool With The PEMs
	for _, rootPEM := range rootPEMs {
		ok := certPool.AppendCertsFromPEM([]byte(rootPEM))
		if !ok {
			return saramaConfigYamlString, nil, fmt.Errorf("failed to parse root certificate PEM: %s", rootPEM)
		}
	}

	// Remove The RootPEMs From The Sarama YAML String (Multi-Line / Greedy To Collect All PEMs)
	updatedSaramaConfigYamlBytes := regexRootPEMs.ReplaceAll([]byte(saramaConfigYamlString), []byte{})
	return string(updatedSaramaConfigYamlBytes), certPool, nil
}
