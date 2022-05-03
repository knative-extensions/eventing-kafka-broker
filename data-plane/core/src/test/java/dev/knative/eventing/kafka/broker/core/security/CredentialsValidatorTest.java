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
  public void securityProtocolSslValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.userCertificate()).thenReturn("abc");
    when(credential.userKey()).thenReturn("key");
    when(credential.caCertificates()).thenReturn("xyz");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocolSslInvalidNoUserCert() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.userCertificate()).thenReturn("   ");
    when(credential.userKey()).thenReturn("my-key");
    when(credential.caCertificates()).thenReturn("xyz");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSslInvalidNoUserKey() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.userCertificate()).thenReturn("xyz");
    when(credential.userKey()).thenReturn("  ");
    when(credential.caCertificates()).thenReturn("my-cert");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSslNoCACert() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.userCertificate()).thenReturn("xyz");
    when(credential.userKey()).thenReturn("my-key");
    when(credential.caCertificates()).thenReturn(null);

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocolSslNoCACertNoClientAuth() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SSL);
    when(credential.skipClientAuth()).thenReturn(true);
    when(credential.userCertificate()).thenReturn("  ");
    when(credential.userKey()).thenReturn("  ");
    when(credential.caCertificates()).thenReturn(null);

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }


  @Test
  public void securityProtocolSaslPlaintextScramSha256Valid() {
    securityProtocolSaslPlaintextValid("SCRAM-SHA-256");
  }

  @Test
  public void securityProtocolSaslPlaintextScramSha512Valid() {
    securityProtocolSaslPlaintextValid("SCRAM-SHA-512");
  }

  @Test
  public void securityProtocolSaslPlaintextPlainValid() {
    securityProtocolSaslPlaintextValid("PLAIN");
  }

  @Test
  public void securityProtocolSaslPlaintextDefauledPlainValid() {
    securityProtocolSaslPlaintextValid(null);
  }

  private static void securityProtocolSaslPlaintextValid(final String mechanism) {

    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    if(mechanism != null) {
      when(credential.SASLMechanism()).thenReturn(mechanism);
    }
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocolSaslPlaintextScramSha513InValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-513");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSaslPLAINTEXT_ScramSha51NoUsernameInValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("  ");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSaslPLAINTEXT_ScramSha51NoPasswordInValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("  ");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSaslSslScramSha256Valid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.caCertificates()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-256");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocolSaslPlainSslValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.caCertificates()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("PLAIN");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocolSaslDefaultedPlainSslValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.caCertificates()).thenReturn("xyz");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocolSaslSslScramSha513InValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.caCertificates()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-513");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSaslSslScramSha51NoUsernameInValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.userCertificate()).thenReturn("abc");
    when(credential.caCertificates()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("  ");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSaslSslScramSha51NoPasswordInValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.userCertificate()).thenReturn("abc");
    when(credential.caCertificates()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("  ");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSaslSslScramSha51EmptyCaCert() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.caCertificates()).thenReturn("   ");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("ccc");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocolSaslSslScramSha51Valid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.userCertificate()).thenReturn("abc");
    when(credential.caCertificates()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("aaa");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocolSaslSslScramSha256NoUsernameInValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.userCertificate()).thenReturn("abc");
    when(credential.caCertificates()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-256");
    when(credential.SASLUsername()).thenReturn("  ");
    when(credential.SASLPassword()).thenReturn("bbb");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSaslSslScramSha256NoPasswordInValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.caCertificates()).thenReturn("xyz");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("  ");

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }

  @Test
  public void securityProtocolSaslSslScramSha256NoCaCert() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.caCertificates()).thenReturn(null);
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("ccc");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocolSaslSslScramSha256EmptyCACert() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
    when(credential.caCertificates()).thenReturn("  ");
    when(credential.SASLMechanism()).thenReturn("SCRAM-SHA-512");
    when(credential.SASLUsername()).thenReturn("bbb");
    when(credential.SASLPassword()).thenReturn("ccc");

    assertThat(CredentialsValidator.validate(credential)).isNull();
  }

  @Test
  public void securityProtocol_nullInValid() {
    final var credential = mock(Credentials.class);

    when(credential.securityProtocol()).thenReturn(null);

    assertThat(CredentialsValidator.validate(credential)).isNotEmpty();
  }
}
