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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

class KubernetesAuthProvider implements AuthProvider {

  private final KubernetesClient kubernetesClient;

  KubernetesAuthProvider(final KubernetesClient client) {
    this.kubernetesClient = client;
  }

  @Override
  public Future<Credentials> getCredentials(final String namespace, final String name) {
    return Vertx.currentContext().executeBlocking(p -> {
      Secret secret;
      try {
        secret = kubernetesClient.secrets()
          .inNamespace(namespace)
          .withName(name)
          .get();
      } catch (final Exception ex) {
        p.fail(ex);
        return;
      }
      if (secret == null) {
        p.fail(String.format("Secret %s/%s null", namespace, name));
        return;
      }

      final var credentials = new KubernetesCredentials(secret);
      final var error = CredentialsValidator.validate(credentials);
      if (error != null) {
        p.fail(error);
        return;
      }
      p.complete(credentials);
    });
  }
}
