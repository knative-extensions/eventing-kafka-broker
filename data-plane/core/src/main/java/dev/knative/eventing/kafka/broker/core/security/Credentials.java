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

public interface Credentials {

    /**
     * @return CA certificate.
     */
    String caCertificates();

    /**
     * Skip client auth.
     *
     * @return true if client auth should be skipped otherwise false
     */
    boolean skipClientAuth();

    /**
     * @return user certificate.
     */
    String userCertificate();

    /**
     * @return user key.
     */
    String userKey();

    /**
     * Client key: security.protocol
     *
     * @return Security protocol or null if not specified.
     */
    SecurityProtocol securityProtocol();

    /**
     * Client key: sasl.mechanism
     *
     * @return SASL mechanism or null if not specified.
     */
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
    String SASLPassword();

    /**
     * Client key: sasl.jaas.config
     *
     * <p>When set, this value is used verbatim as the JAAS configuration for the Kafka client.
     * Primarily useful for OAUTHBEARER, where the operator supplies a custom login module
     * and its configuration (e.g. scope, token endpoint).
     *
     * @return the full JAAS config string, or null if not specified.
     */
    default String SASLJaasConfig() {
        return null;
    }

    /**
     * Client key: sasl.login.callback.handler.class
     *
     * <p>Fully qualified class name of an {@code AuthenticateCallbackHandler} implementation
     * that the Kafka client will use to obtain tokens. The class must be on the data-plane
     * classpath — this project does not ship any vendor-specific handler.
     *
     * @return the handler class name, or null if not specified.
     */
    default String SASLLoginCallbackHandlerClass() {
        return null;
    }
}
