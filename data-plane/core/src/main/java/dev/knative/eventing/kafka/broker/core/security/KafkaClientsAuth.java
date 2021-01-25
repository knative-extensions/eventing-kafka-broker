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

import io.vertx.core.Future;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;

import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

public class KafkaClientsAuth {

  public static Future<Properties> updateConfigsFromProps(final Credentials credentials,
                                                          final Properties properties) {
    return clientsProperties(properties::setProperty, credentials)
      .map(r -> properties);
  }

  public static Future<Map<String, String>> updateProducerConfigs(final Credentials credentials,
                                                                  final Map<String, String> configs) {
    return clientsProperties(configs::put, credentials)
      .map(r -> configs);
  }

  public static Future<Map<String, Object>> updateConsumerConfigs(final Credentials credentials,
                                                                  final Map<String, Object> configs) {
    return clientsProperties(configs::put, credentials)
      .map(r -> configs);
  }

  private static Future<Void> clientsProperties(final BiConsumer<String, String> propertiesSetter, final Credentials credentials) {
    final var protocol = credentials.securityProtocol();
    if (protocol == null) {
      return Future.succeededFuture();
    }

    propertiesSetter.accept(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.name);
    return switch (protocol) {
      case PLAINTEXT -> Future.succeededFuture();
      case SSL -> ssl(propertiesSetter, credentials);
      case SASL_PLAINTEXT -> sasl(propertiesSetter, credentials);
      case SASL_SSL -> ssl(propertiesSetter, credentials).compose(r -> sasl(propertiesSetter, credentials));
    };
  }

  private static Future<Void> sasl(final BiConsumer<String, String> propertiesSetter, final Credentials credentials) {
    final var mechanism = credentials.SASLMechanism();
    if (mechanism == null) {
      return Future.failedFuture("SASL mechanism required");
    }
    propertiesSetter.accept(SaslConfigs.SASL_MECHANISM, mechanism);
    if ("PLAIN".equals(mechanism)) {
      propertiesSetter.accept(SaslConfigs.SASL_JAAS_CONFIG, String.format(
        PlainLoginModule.class.getName() + " required username=\"%s\" password=\"%s\";",
        credentials.SASLUsername(),
        credentials.SASLPassword()
      ));
    } else {
      propertiesSetter.accept(SaslConfigs.SASL_JAAS_CONFIG, String.format(
        ScramLoginModule.class.getName() + " required username=\"%s\" password=\"%s\";",
        credentials.SASLUsername(),
        credentials.SASLPassword()
      ));
    }
    return Future.succeededFuture();
  }

  private static Future<Void> ssl(final BiConsumer<String, String> propertiesSetter, final Credentials credentials) {
    propertiesSetter.accept(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
    propertiesSetter.accept(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, credentials.caCertificates());
    final var keystore = credentials.userCertificate();
    if (keystore != null) {
      propertiesSetter.accept(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, credentials.userCertificate());
      propertiesSetter.accept(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, credentials.userKey());
      propertiesSetter.accept(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
    }
    return Future.succeededFuture();
  }
}
