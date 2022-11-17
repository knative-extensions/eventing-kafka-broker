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

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class KubernetesCredentialsTest {

  @Test
  public void getKubernetesCredentialsSaslMechanismPlain() {
    testGetKubernetesCredentialsForSaslMechanism("PLAIN");
  }

  @Test
  public void getKubernetesCredentialsSaslMechanismScram256() {
    testGetKubernetesCredentialsForSaslMechanism("SCRAM-SHA-256");
  }

  @Test
  public void getKubernetesCredentialsSaslMechanismScram512() {
    testGetKubernetesCredentialsForSaslMechanism("SCRAM-SHA-512");
  }

  @Test
  public void getKubernetesCredentialsSkipUserTrue() {
    final var credentials = getKubernetesCredentialsFromSecretWithSkipUser("true");

    assertThat(credentials.skipClientAuth()).isEqualTo(true);
  }

  @Test
  public void getKubernetesCredentialsSkipUserFalse() {
    final var credentials = getKubernetesCredentialsFromSecretWithSkipUser("false");

    assertThat(credentials.skipClientAuth()).isEqualTo(false);
  }

  @Test
  public void getKubernetesCredentialsSkipUserEmpty() {
    final var credentials = getKubernetesCredentialsFromSecretWithSkipUser("");

    assertThat(credentials.skipClientAuth()).isEqualTo(false);
  }

  @Test
  public void getKubernetesCredentialsFromEmptySecret() {
    final var credentials = getKubernetesCredentialsFromSecretData(new HashMap<>());

    assertThat(credentials.securityProtocol()).isNull();
    assertAll(new HashMap<>(), credentials);
  }

  @Test
  public void getKubernetesCredentialsFromNullSecret() {
    Secret secret = null;
    final var credentials = new KubernetesCredentials(secret);;

    assertThat(credentials.securityProtocol()).isNull();
    assertAll(new HashMap<>(), credentials);
  }


  @Test
  public void getKubernetesCredentialsFromNullSecretData() {
    final var credentials = new KubernetesCredentials(
      new SecretBuilder()
        .withNewMetadata()
        .withNamespace("ns1")
        .withName("name1")
        .endMetadata()
        .build()
    );

    assertThat(credentials.securityProtocol()).isNull();
    assertAll(new HashMap<>(), credentials);
  }

  @Test
  public void unknownSecurityProtocolReturnsNull() {
    final var data = Map.of(
      KubernetesCredentials.SECURITY_PROTOCOL, "SASSO_PLAINTEXT"
    );

    final var credentials = getKubernetesCredentialsFromSecretData(data);

    assertThat(credentials.securityProtocol()).isNull();
  }

  private static KubernetesCredentials getKubernetesCredentialsFromSecretData(Map<String, String> data) {
    return new KubernetesCredentials(
      new SecretBuilder()
        .withNewMetadata()
        .withNamespace("ns1")
        .withName("name1")
        .endMetadata()
        .withData(
          base64(data)
        )
        .build()
    );
  }

  private static Map<String, String> getKubernetesCredentialsSecretDataForSaslMechanism(final String saslMechanism) {
    return Map.of(
      KubernetesCredentials.SASL_MECHANISM, saslMechanism
    );
  }

  private static void testGetKubernetesCredentialsForSaslMechanism(final String saslMechanism) {
    final var data = getKubernetesCredentialsSecretDataForSaslMechanism(saslMechanism);
    final var credentials = getKubernetesCredentialsFromSecretData(data);

    assertThat(credentials.SASLMechanism()).isEqualTo(saslMechanism);
  }

  private static KubernetesCredentials getKubernetesCredentialsFromSecretWithSkipUser(String skipUser) {
    final var data = Map.of(
      KubernetesCredentials.CA_CERTIFICATE_KEY, "CA_CERT",
      KubernetesCredentials.USER_CERTIFICATE_KEY, "USER_CERT",
      KubernetesCredentials.USER_KEY_KEY, "USER_KEY",
      KubernetesCredentials.SASL_MECHANISM, "PLAIN",
      KubernetesCredentials.SECURITY_PROTOCOL, SecurityProtocol.SASL_SSL.name,
      KubernetesCredentials.USERNAME_KEY, "USERNAME",
      KubernetesCredentials.PASSWORD_KEY, "PASSWORD",
      KubernetesCredentials.USER_SKIP_KEY, skipUser
    );

    return getKubernetesCredentialsFromSecretData(data);
  }

  private static void assertAll(final Map<String, String> data, final KubernetesCredentials credentials) {
    assertThat(credentials.SASLMechanism()).isEqualTo(data.get(KubernetesCredentials.SASL_MECHANISM));
    assertThat(credentials.caCertificates()).isEqualTo(data.get(KubernetesCredentials.CA_CERTIFICATE_KEY));
    assertThat(credentials.userCertificate()).isEqualTo(data.get(KubernetesCredentials.USER_CERTIFICATE_KEY));
    assertThat(credentials.userKey()).isEqualTo(data.get(KubernetesCredentials.USER_KEY_KEY));
    assertThat(credentials.SASLUsername()).isEqualTo(data.get(KubernetesCredentials.USERNAME_KEY));
    assertThat(credentials.SASLPassword()).isEqualTo(data.get(KubernetesCredentials.PASSWORD_KEY));
  }

  private static Map<String, String> base64(Map<String, String> data) {
    return data.entrySet().stream()
      .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), Base64.getEncoder().encodeToString(e.getValue().getBytes())))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
