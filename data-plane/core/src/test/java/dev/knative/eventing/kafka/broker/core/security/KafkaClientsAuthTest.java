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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.junit.jupiter.api.Test;

import javax.security.auth.spi.LoginModule;
import java.util.HashMap;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaClientsAuthTest {

  @Test
  public void shouldConfigureSaslScram512Ssl() {
    shouldConfigureSaslSsl(ScramLoginModule.class, "SCRAM-SHA-512");
  }

  @Test
  public void shouldConfigureSaslScram256Ssl() {
    shouldConfigureSaslSsl(ScramLoginModule.class, "SCRAM-SHA-256");
  }

  @Test
  public void shouldConfigureSaslPlainSsl() {
    shouldConfigureSaslSsl(PlainLoginModule.class, "PLAIN");
  }

  private static void shouldConfigureSaslSsl(final Class<? extends LoginModule> module, final String mechanism) {
    final var props = new Properties();

    final var credentials = mock(Credentials.class);
    when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credentials.caCertificates()).thenReturn("xyz");
    when(credentials.SASLMechanism()).thenReturn(mechanism);
    when(credentials.SASLUsername()).thenReturn("aaa");
    when(credentials.SASLPassword()).thenReturn("bbb");

    assertThat(KafkaClientsAuth.updateConfigsFromProps(credentials, props).succeeded()).isTrue();

    final var expected = new Properties();
    expected.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());
    expected.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
    expected.setProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "xyz");
    expected.setProperty(SaslConfigs.SASL_MECHANISM, mechanism);
    expected.setProperty(
      SaslConfigs.SASL_JAAS_CONFIG,
      module.getName() + " required username=\"" + credentials.SASLUsername() + "\" password=\"" + credentials.SASLPassword() + "\";"
    );

    assertThat(props).isEqualTo(expected);

    final var producerConfigs = new HashMap<String, String>();
    final var consumerConfigs = new HashMap<String, Object>();

    assertThat(KafkaClientsAuth.updateProducerConfigs(credentials, producerConfigs).succeeded()).isTrue();
    assertThat(KafkaClientsAuth.updateConsumerConfigs(credentials, consumerConfigs).succeeded()).isTrue();

    assertThat(producerConfigs).isEqualTo(expected);
    assertThat(consumerConfigs).isEqualTo(expected);
  }

  @Test
  public void shouldConfigureSsl() {
    final var props = new Properties();

    final var credentials = mock(Credentials.class);
    when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credentials.userCertificate()).thenReturn("abc");
    when(credentials.userKey()).thenReturn("key");
    when(credentials.caCertificates()).thenReturn("xyz");

    assertThat(KafkaClientsAuth.updateConfigsFromProps(credentials, props).succeeded()).isTrue();

    final var expected = new Properties();
    expected.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    expected.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
    expected.setProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "xyz");
    expected.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
    expected.setProperty(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "abc");
    expected.setProperty(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, "key");

    assertThat(props).isEqualTo(expected);

    final var producerConfigs = new HashMap<String, String>();
    final var consumerConfigs = new HashMap<String, Object>();

    assertThat(KafkaClientsAuth.updateProducerConfigs(credentials, producerConfigs).succeeded()).isTrue();
    assertThat(KafkaClientsAuth.updateConsumerConfigs(credentials, consumerConfigs).succeeded()).isTrue();

    assertThat(producerConfigs).isEqualTo(expected);
    assertThat(consumerConfigs).isEqualTo(expected);
  }

  @Test
  public void shouldConfigureSaslPlaintextScram512() {
    shouldConfigureSaslPlaintext("SCRAM-SHA-512");
  }

  @Test
  public void shouldConfigureSaslPlaintextScram256() {
    shouldConfigureSaslPlaintext("SCRAM-SHA-256");
  }

  @Test
  public void shouldConfigurePlaintext() {
    final var props = new Properties();

    final var credentials = mock(Credentials.class);
    when(credentials.securityProtocol()).thenReturn(SecurityProtocol.PLAINTEXT);

    assertThat(KafkaClientsAuth.updateConfigsFromProps(credentials, props).succeeded()).isTrue();

    final var expected = new Properties();
    expected.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());

    assertThat(props).isEqualTo(expected);

    final var producerConfigs = new HashMap<String, String>();
    final var consumerConfigs = new HashMap<String, Object>();

    assertThat(KafkaClientsAuth.updateProducerConfigs(credentials, producerConfigs).succeeded()).isTrue();
    assertThat(KafkaClientsAuth.updateConsumerConfigs(credentials, consumerConfigs).succeeded()).isTrue();

    assertThat(producerConfigs).isEqualTo(expected);
    assertThat(consumerConfigs).isEqualTo(expected);
  }

  private static void shouldConfigureSaslPlaintext(final String mechanism) {
    final var props = new Properties();

    final var credentials = mock(Credentials.class);
    when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    when(credentials.SASLMechanism()).thenReturn(mechanism);
    when(credentials.SASLUsername()).thenReturn("aaa");
    when(credentials.SASLPassword()).thenReturn("bbb");

    assertThat(KafkaClientsAuth.updateConfigsFromProps(credentials, props).succeeded()).isTrue();

    final var expected = new Properties();

    expected.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
    expected.setProperty(SaslConfigs.SASL_MECHANISM, mechanism);
    expected.setProperty(
      SaslConfigs.SASL_JAAS_CONFIG,
      ScramLoginModule.class.getName() + " required username=\"" + credentials.SASLUsername() + "\" password=\"" + credentials.SASLPassword() + "\";"
    );

    assertThat(props).isEqualTo(expected);

    final var producerConfigs = new HashMap<String, String>();
    final var consumerConfigs = new HashMap<String, Object>();

    assertThat(KafkaClientsAuth.updateProducerConfigs(credentials, producerConfigs).succeeded()).isTrue();
    assertThat(KafkaClientsAuth.updateConsumerConfigs(credentials, consumerConfigs).succeeded()).isTrue();

    assertThat(producerConfigs).isEqualTo(expected);
    assertThat(consumerConfigs).isEqualTo(expected);
  }
}
