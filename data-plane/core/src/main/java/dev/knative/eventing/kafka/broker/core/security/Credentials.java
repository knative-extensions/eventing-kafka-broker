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

/**
 * Credentials validation:
 * <blockquote><pre>{@code
 * - security.protocol=PLAINTEXT
 * - security.protocol=SSL
 *   - truststore is not empty
 *   - keystore is not empty
 * - security.protocol=SASL_PLAINTEXT
 *   - SASL mechanism is valid (SCRAM-SHA-256 or SCRAM-SHA-512)
 *   - SASL username is not empty
 *   - SASL password is not empty
 * - security.protocol=SASL_SSL
 *   - truststore is not empty
 *   - keystore is not empty
 *   - SASL mechanism is valid (SCRAM-SHA-256 or SCRAM-SHA-512)
 *   - SASL username is not empty
 *   - SASL password is not empty
 * }</pre></blockquote>
 * <p>
 * Since our components are multi tenant in case SSL is configured
 * client authentication is required even though is optional for Kafka.
 */
public interface Credentials {

  /**
   * @return truststore content.
   */
  @Nullable
  String truststore();

  /**
   * Client key: ssl.truststore.password
   *
   * @return truststore password.
   */
  @Nullable
  String truststorePassword();

  /**
   * @return truststore path.
   */
  @Nullable
  String truststorePath();

  /**
   * @return keystore content.
   */
  @Nullable
  String keystore();

  /**
   * Client keys:
   * - ssl.keystore.password
   * - ssl.key.password
   *
   * @return keystore password.
   */
  @Nullable
  String keystorePassword();

  /**
   * @return keystore path.
   */
  @Nullable
  String keystorePath();

  /**
   * Client key: security.protocol
   * Possible values:
   * - PLAINTEXT
   * - SSL
   * - SASL_PLAINTEXT
   * - SASL_SSL
   *
   * @return Security protocol or null if not specified.
   */
  @Nullable
  SecurityProtocol securityProtocol();

  /**
   * Client key: sasl.mechanism
   * Possible values:
   * - SCRAM-SHA-256
   * - SCRAM-SHA-512
   *
   * @return SASL mechanism or null if not specified.
   */
  @Nullable
  String SASLMechanism();

  /**
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
