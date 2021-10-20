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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class KubernetesAuthProvider implements AuthProvider {

  private final KubernetesClient kubernetesClient;

  KubernetesAuthProvider(final KubernetesClient client) {
    this.kubernetesClient = client;
  }

  private Future<Credentials> getCredentials(final DataPlaneContract.Reference secretReference) {
    return Vertx.currentContext().executeBlocking(p -> {
      try {
        final Secret secret = getSecretFromKubernetes(secretReference);
        final var credentials = new KubernetesCredentials(secret);
        final var error = CredentialsValidator.validate(credentials);
        if (error != null) {
          p.fail(error);
          return;
        }
        p.complete(credentials);
      } catch (final Exception ex) {
        p.fail(ex);
      }
    });
  }

  private Future<Credentials> getCredentials(final DataPlaneContract.MultiSecretReference secretReferences) {
    return Vertx.currentContext().executeBlocking(p -> {
      try {
        final var credentials = new KubernetesCredentials(secretDataOf(secretReferences));
        final var error = CredentialsValidator.validate(credentials);
        if (error != null) {
          p.fail(error);
          return;
        }
        p.complete(credentials);
      } catch (final Exception ex) {
        p.fail(ex);
      }
    });
  }

  private Map<String, String> secretDataOf(final DataPlaneContract.MultiSecretReference secretReferences) {
    // For each secret get the secret from Kubernetes and populate the finalSecretData variable with the expected
    // fields.
    final var secretData = new HashMap<String, String>(secretReferences.getReferencesCount() * 2);
    secretData.put(KubernetesCredentials.SECURITY_PROTOCOL, protocolContractToSecurityProtocol(secretReferences.getProtocol()));
    for (final var secretReference : secretReferences.getReferencesList()) {
      final var objectReference = secretReference.getReference();
      final Secret secret = getSecretFromKubernetes(objectReference);
      for (final var keyFieldReference : secretReference.getKeyFieldReferencesList()) {
        final var field = keyFieldReference.getField();
        final var key = keyFieldReference.getSecretKey();
        secretData.put(fieldContractToCredentialsString(field), secret.getData().get(key));
      }
    }
    return secretData;
  }

  @Override
  public Future<Credentials> getCredentials(final DataPlaneContract.Resource resource) {
    if (resource.hasAbsentAuth() || DataPlaneContract.Resource.AuthCase.AUTH_NOT_SET.equals(resource.getAuthCase())) {
      return Future.succeededFuture(new PlaintextCredentials());
    }
    if (resource.hasAuthSecret()) {
      return getCredentials(resource.getAuthSecret());
    }
    if (resource.hasMultiAuthSecret()) {
      return getCredentials(resource.getMultiAuthSecret());
    }
    throw new IllegalStateException("Unknown auth state for resource " + resource);
  }

  private Secret getSecretFromKubernetes(final DataPlaneContract.Reference secretReference) {
    return kubernetesClient.secrets()
      .inNamespace(secretReference.getNamespace())
      .withName(secretReference.getName())
      .withResourceVersion(secretReference.getVersion())
      .waitUntilReady(5, TimeUnit.SECONDS);
  }

  private static String protocolContractToSecurityProtocol(final DataPlaneContract.Protocol protocol) {
    final var protocolStr = switch (protocol) {
      case PLAINTEXT -> SecurityProtocol.PLAINTEXT.name;
      case SSL -> SecurityProtocol.SSL.name;
      case SASL_PLAINTEXT -> SecurityProtocol.SASL_PLAINTEXT.name;
      case SASL_SSL -> SecurityProtocol.SASL_SSL.name;
      case UNRECOGNIZED -> throw new IllegalArgumentException("unknown protocol " + protocol);
    };
    return Base64.getEncoder().encodeToString(protocolStr.getBytes(StandardCharsets.UTF_8));
  }

  private static String fieldContractToCredentialsString(final DataPlaneContract.SecretField field) {
    return switch (field) {
      case SASL_MECHANISM -> KubernetesCredentials.SASL_MECHANISM;
      case CA_CRT -> KubernetesCredentials.CA_CERTIFICATE_KEY;
      case USER_CRT -> KubernetesCredentials.USER_CERTIFICATE_KEY;
      case USER_KEY -> KubernetesCredentials.USER_KEY_KEY;
      case USER -> KubernetesCredentials.USERNAME_KEY;
      case PASSWORD -> KubernetesCredentials.PASSWORD_KEY;
      case UNRECOGNIZED -> throw new IllegalArgumentException("unknown field mapping for " + field);
    };
  }
}
