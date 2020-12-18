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
import org.apache.kafka.common.security.auth.SecurityProtocol;

import javax.annotation.Nullable;

class KubernetesCredentials implements Credentials {

  static final String CA_STORE_KEY = "ca.p12";
  static final String CA_STORE_PASSWORD_KEY = "ca.password";

  static final String USER_STORE_KEY = "user.p12";
  static final String USER_STORE_PASSWORD_KEY = "user.password";

  static final String USERNAME_KEY = "user";
  static final String PASSWORD_KEY = "password";

  static final String SECURITY_PROTOCOL = "protocol";
  static final String SASL_MECHANISM = "sasl.mechanism";

  private final Secret secret;
  private final String truststorePath;
  private final String keystorePath;

  KubernetesCredentials(final Secret secret) {
    this.secret = secret;
    final var dir = String.format("/%s/%s", secret.getMetadata().getNamespace(), secret.getMetadata().getName());
    this.truststorePath = Stores.truststorePath(dir);
    this.keystorePath = Stores.keystorePath(dir);
  }

  @Override
  @Nullable
  public String truststore() {
    return secret.getData().get(CA_STORE_KEY);
  }

  @Override
  @Nullable
  public String truststorePassword() {
    return secret.getData().get(CA_STORE_PASSWORD_KEY);
  }

  @Override
  @Nullable
  public String truststorePath() {
    return this.truststorePath;
  }

  @Override
  @Nullable
  public String keystore() {
    return secret.getData().get(USER_STORE_KEY);
  }

  @Override
  @Nullable
  public String keystorePassword() {
    return secret.getData().get(USER_STORE_PASSWORD_KEY);
  }

  @Override
  public String keystorePath() {
    return this.keystorePath;
  }

  @Override
  @Nullable
  public SecurityProtocol securityProtocol() {
    final var protocol = secret.getData().get(SECURITY_PROTOCOL);
    if (!SecurityProtocol.names().contains(protocol)) {
      return null;
    }
    return SecurityProtocol.forName(protocol);
  }

  @Override
  @Nullable
  public String SASLMechanism() {
    final var mechanism = secret.getData().get(SASL_MECHANISM);
    if (mechanism == null) {
      return null;
    }
    return switch (mechanism) {
      case "SCRAM-SHA-256" -> "SCRAM-SHA-256";
      case "SCRAM-SHA-512" -> "SCRAM-SHA-512";
      default -> null;
    };
  }

  @Override
  @Nullable
  public String SASLUsername() {
    return secret.getData().get(USERNAME_KEY);
  }

  @Override
  @Nullable
  public String SASLPassword() {
    return secret.getData().get(PASSWORD_KEY);
  }
}
