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

import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

import java.util.AbstractMap;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
@EnableRuleMigrationSupport
public class KubernetesAuthProviderTest {

  @Rule
  public KubernetesServer server = new KubernetesServer(true, true);

  @Test
  public void getCredentialsFromSecret(final Vertx vertx, final VertxTestContext context) {
    final var client = server.getClient();

    final var data = new HashMap<String, String>();

    data.put(KubernetesCredentials.SECURITY_PROTOCOL, SecurityProtocol.SASL_SSL.name);
    data.put(KubernetesCredentials.SASL_MECHANISM, "SCRAM-SHA-512");

    data.put(KubernetesCredentials.USER_CERTIFICATE_KEY, "my-user-cert");
    data.put(KubernetesCredentials.USER_KEY_KEY, "my-user-key");

    data.put(KubernetesCredentials.USERNAME_KEY, "my-username");
    data.put(KubernetesCredentials.PASSWORD_KEY, "my-user-password");

    data.put(KubernetesCredentials.CA_CERTIFICATE_KEY, "my-ca-certificate");

    final var secret = new SecretBuilder()
      .withNewMetadata()
      .withName("my-secret-name")
      .withNamespace("my-secret-namespace")
      .endMetadata()
      .withData(
        data.entrySet().stream()
          .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), Base64.getEncoder().encodeToString(e.getValue().getBytes())))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
      )
      .build();

    client.secrets().inNamespace("my-secret-namespace").create(secret);

    final var provider = new KubernetesAuthProvider(server.getClient());

    vertx.runOnContext(r -> {
      final var credentialsFuture = provider.getCredentials("my-secret-namespace", "my-secret-name");

      credentialsFuture
        .onFailure(context::failNow)
        .onSuccess(credentials -> context.verify(() -> {

          assertThat(credentials.SASLMechanism()).isEqualTo(data.get(KubernetesCredentials.SASL_MECHANISM));
          assertThat(credentials.securityProtocol()).isEqualTo(SecurityProtocol.forName(data.get(KubernetesCredentials.SECURITY_PROTOCOL)));

          assertThat(credentials.userCertificate()).isEqualTo(data.get(KubernetesCredentials.USER_CERTIFICATE_KEY));

          assertThat(credentials.caCertificates()).isEqualTo(data.get(KubernetesCredentials.CA_CERTIFICATE_KEY));

          context.completeNow();
        }));
    });
  }

  @Test
  public void shouldFailOnSecretNotFound(final Vertx vertx, final VertxTestContext context) {

    final var provider = new KubernetesAuthProvider(server.getClient());

    vertx.runOnContext(r -> provider.getCredentials("my-secret-namespace", "my-secret-name-not-found")
      .onSuccess(ignored -> context.failNow("Unexpected success: expected not found error"))
      .onFailure(cause -> context.completeNow())
    );
  }

  @Test
  public void shouldFailOnInvalidSecret(final Vertx vertx, final VertxTestContext context) {

    final var provider = new KubernetesAuthProvider(server.getClient());

    final var data = new HashMap<String, String>();

    data.put(KubernetesCredentials.SECURITY_PROTOCOL, SecurityProtocol.SASL_SSL.name);
    data.put(KubernetesCredentials.SASL_MECHANISM, "SCRAM-SHA-512");

    final var secret = new SecretBuilder()
      .withNewMetadata()
      .withName("my-secret-name-invalid")
      .withNamespace("my-secret-namespace")
      .endMetadata()
      .withData(
        data.entrySet().stream()
          .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), Base64.getEncoder().encodeToString(e.getValue().getBytes())))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
      )
      .build();

    server.getClient().secrets().inNamespace("my-secret-namespace").create(secret);

    vertx.runOnContext(r -> provider.getCredentials("my-secret-namespace", "my-secret-name-invalid")
      .onSuccess(ignored -> context.failNow("Unexpected success: expected invalid secret error"))
      .onFailure(cause -> context.completeNow())
    );
  }
}
