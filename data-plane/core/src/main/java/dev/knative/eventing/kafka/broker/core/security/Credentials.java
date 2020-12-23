/*
 * Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
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

package dev.knative.eventing.kafka.broker.core.security;

import org.apache.kafka.common.security.auth.SecurityProtocol;

import javax.annotation.Nullable;

public interface Credentials {

  /**
   * @return CA certificate.
   */
  @Nullable
  String caCertificates();

  /**
   * @return user certificate.
   */
  @Nullable
  String userCertificate();

  /**
   * @return user key.
   */
  @Nullable
  String userKey();

  /**
   * Client key: security.protocol
   *
   * @return Security protocol or null if not specified.
   */
  @Nullable
  SecurityProtocol securityProtocol();

  /**
   * Client key: sasl.mechanism
   *
   * @return SASL mechanism or null if not specified.
   */
  @Nullable
  String SASLMechanism();

  /**
   * Client config:
   * sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
   * username="alice" \
   * password="alice-secret";
   *
   * @return username.
   * @see <a href="https://kafka.apache.org/documentation/#security_sasl_scram">SASL Scram</a>
   */
  @Nullable
  String SASLUsername();

  /**
   * Client config:
   * sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
   * username="alice" \
   * password="alice-secret";
   *
   * @return password.
   * @see <a href="https://kafka.apache.org/documentation/#security_sasl_scram">SASL Scram</a>
   */
  @Nullable
  String SASLPassword();
}
