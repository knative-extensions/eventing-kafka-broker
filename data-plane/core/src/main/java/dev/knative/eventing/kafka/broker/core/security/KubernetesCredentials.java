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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

class KubernetesCredentials implements Credentials {

  private final static Logger logger = LoggerFactory.getLogger(KubernetesCredentials.class);

  static final String CA_CERTIFICATE_KEY = "ca.crt";

  static final String USER_CERTIFICATE_KEY = "user.crt";
  static final String USER_KEY_KEY = "user.key";
  static final String USER_SKIP_KEY = "user.skip";

  static final String USERNAME_KEY = "user";
  static final String PASSWORD_KEY = "password";

  static final String SECURITY_PROTOCOL = "protocol";
  static final String SASL_MECHANISM = "sasl.mechanism";

  private final Secret secret;

  private String caCertificates;
  private String userCertificate;
  private String userKey;
  private Boolean skipUser;
  private SecurityProtocol securityProtocol;
  private String SASLMechanism;
  private String SASLUsername;
  private String SASLPassword;

  KubernetesCredentials(final Secret secret) {
    this.secret = secret;
  }

  @Override
  public String caCertificates() {
    if (secret == null || secret.getData() == null) {
      return null;
    }
    if (caCertificates == null) {
      final var truststore = secret.getData().get(CA_CERTIFICATE_KEY);
      if (truststore == null) {
        return null;
      }
      this.caCertificates = new String(Base64.getDecoder().decode(truststore));
    }
    return this.caCertificates;
  }

  @Override
  public boolean skipClientAuth() {
    if (secret == null || secret.getData() == null) {
      return false;
    }
    if (skipUser == null) {
      final var skip = secret.getData().get(USER_SKIP_KEY);
      if (skip == null) {
        this.skipUser = false;
      } else {
        try {
          this.skipUser = Boolean.parseBoolean(skip);
        } catch (final Exception ex) {
          this.skipUser = false;
        }
      }
    }
    return this.skipUser;
  }

  @Override
  public String userCertificate() {
    if (secret == null || secret.getData() == null) {
      return null;
    }
    if (userCertificate == null) {
      final var keystore = secret.getData().get(USER_CERTIFICATE_KEY);
      if (keystore == null) {
        return null;
      }
      this.userCertificate = new String(Base64.getDecoder().decode(keystore));
    }
    return userCertificate;
  }

  @Override
  public String userKey() {
    if (secret == null || secret.getData() == null) {
      return null;
    }
    if (userKey == null) {
      final var userKey = secret.getData().get(USER_KEY_KEY);
      if (userKey == null) {
        return null;
      }
      this.userKey = new String(Base64.getDecoder().decode(userKey));
    }
    return userKey;
  }


  @Override
  public SecurityProtocol securityProtocol() {
    if (secret == null || secret.getData() == null) {
      return null;
    }
    if (securityProtocol == null) {
      final var protocolStr = secret.getData().get(SECURITY_PROTOCOL);
      if (protocolStr == null) {
        return null;
      }
      final var protocol = new String(Base64.getDecoder().decode(protocolStr));
      if (!SecurityProtocol.names().contains(protocol)) {
        logger.debug("Security protocol {}", keyValue(SECURITY_PROTOCOL, protocol));
        return null;
      }
      this.securityProtocol = SecurityProtocol.forName(protocol);
    }
    return this.securityProtocol;
  }

  @Override
  public String SASLMechanism() {
    if (secret == null || secret.getData() == null) {
      return null;
    }
    if (SASLMechanism == null) {
      final var SASLMechanism = secret.getData().get(SASL_MECHANISM);
      if (SASLMechanism == null) {
        return null;
      }
      this.SASLMechanism = switch (new String(Base64.getDecoder().decode(SASLMechanism))) {
        case "PLAIN"         -> "PLAIN";
        case "SCRAM-SHA-256" -> "SCRAM-SHA-256";
        case "SCRAM-SHA-512" -> "SCRAM-SHA-512";
        default -> null;
      };
    }
    return this.SASLMechanism;
  }

  @Override
  public String SASLUsername() {
    if (secret == null || secret.getData() == null) {
      return null;
    }
    if (SASLUsername == null) {
      final var SASLUsername = secret.getData().get(USERNAME_KEY);
      if (SASLUsername == null) {
        return null;
      }
      this.SASLUsername = new String(Base64.getDecoder().decode(SASLUsername));
    }
    return this.SASLUsername;
  }

  @Override
  public String SASLPassword() {
    if (secret == null || secret.getData() == null) {
      return null;
    }
    if (SASLPassword == null) {
      final var SASLPassword = secret.getData().get(PASSWORD_KEY);
      if (SASLPassword == null) {
        return null;
      }
      this.SASLPassword = new String(Base64.getDecoder().decode(SASLPassword));
    }
    return this.SASLPassword;
  }
}
