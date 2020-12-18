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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CredentialsValidatorTest {

  @Test
  public void securityProtocolPlaintextValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.PLAINTEXT);

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocol_SSL_valid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocol_SSL_invalid_NoKeystore() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.keystore()).thenReturn("   ");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SSL_invalid_NoTruststore() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.keystore()).thenReturn("xyz");
    when(credential.truststore()).thenReturn("   ");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SSL_NoTruststorePassword_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("   ");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SSL_NoKeystorePassword_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.keystorePassword()).thenReturn("  ");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_PLAINTEXT_SCRAM_SHA_256_valid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-256");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocol_SASL_PLAINTEXT_SCRAM_SHA_512_valid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocol_SASL_PLAINTEXT_SCRAM_SHA_513_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-513");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_PLAINTEXT_SCRAM_SHA_512_NoUsername_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("  ");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_PLAINTEXT_SCRAM_SHA_512_NoPassword_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("  ");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_256_valid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-256");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_512_NoKeystorePassword_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");
    when(credential.keystorePassword()).thenReturn("   ");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_512_NoTruststorePassword_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");
    when(credential.keystorePassword()).thenReturn("qwerty");
    when(credential.truststorePassword()).thenReturn("   ");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_513_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-513");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_512_NoUsername_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("  ");
    when(credential.SASLPassword()).thenReturn("bbb");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_512_NoPassword_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("  ");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_512_NoTruststore_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("   ");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("ccc");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_512_NoKeystore_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("  ");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("ccc");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_512_valid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_256_NoKeystorePassword_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-256");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");
    when(credential.keystorePassword()).thenReturn("   ");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_256_NoTruststorePassword_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-256");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");
    when(credential.keystorePassword()).thenReturn("qwerty");
    when(credential.truststorePassword()).thenReturn("   ");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_256_NoUsername_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-256");
    when(credential.SASLUsername()).thenReturn("  ");
    when(credential.SASLPassword()).thenReturn("bbb");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_256_NoPassword_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("  ");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_256_NoTruststore_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("abc");
    when(credential.truststore()).thenReturn("   ");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("ccc");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_SASL_SSL_SCRAM_SHA_256_NoKeystore_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.keystore()).thenReturn("  ");
    when(credential.truststore()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("ccc");
    when(credential.keystorePassword()).thenReturn("qwe");
    when(credential.truststorePassword()).thenReturn("qwerty");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocol_null_invalid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(null);

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }
}
