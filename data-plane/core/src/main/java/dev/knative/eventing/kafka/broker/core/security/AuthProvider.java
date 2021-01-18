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

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.vertx.core.Future;

/**
 * AuthProvider provides auth credentials.
 */
@FunctionalInterface
public interface AuthProvider {

  static AuthProvider kubernetes() {
    return new KubernetesAuthProvider(new DefaultKubernetesClient());
  }

  /**
   * Get credentials from given a location represented by namespace and name.
   *
   * @return A failed or succeeded future with valid credentials.
   */
  Future<Credentials> getCredentials(final String namespace, final String name);
}
