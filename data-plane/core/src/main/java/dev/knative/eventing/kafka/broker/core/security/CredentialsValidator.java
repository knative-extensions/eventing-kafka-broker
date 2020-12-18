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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class CredentialsValidator {

  private CredentialsValidator() {
  }

  @Nullable
  static String validate(@Nonnull final Credentials credentials) {

    final var securityProtocol = credentials.securityProtocol();
    if (securityProtocol == null) {
      return "No security protocol specified";
    }

    if (is(SecurityProtocol.PLAINTEXT, securityProtocol)) {
      return null;
    }

    if (is(SecurityProtocol.SSL, securityProtocol)) {
      if (anyBlank(credentials.keystore(), credentials.truststore())) {
        return "Security protocol " + securityProtocol.name + ": invalid keystore or truststore";
      }
      if (anyBlank(credentials.keystorePassword(), credentials.truststorePassword())) {
        return "Security protocol " + securityProtocol.name + ": invalid password of keystore or truststore";
      }
      return null;
    }

    final var SASLMechanism = credentials.SASLMechanism();
    if (is(SecurityProtocol.SASL_PLAINTEXT, securityProtocol)) {
      if (isInvalidSASLMechanism(SASLMechanism)) {
        return "Security protocol " + securityProtocol.name + ": invalid SASL mechanism, expected SCRAM-SHA-256 or SCRAM-SHA-512 got " + SASLMechanism;
      }
      if (anyBlank(credentials.SASLUsername(), credentials.SASLPassword())) {
        return "Security protocol " + securityProtocol.name + ":  invalid SASL username or password";
      }
      return null;
    }

    if (is(SecurityProtocol.SASL_SSL, securityProtocol)) {
      if (anyBlank(credentials.keystore(), credentials.truststore())) {
        return "Security protocol " + securityProtocol.name + ": invalid keystore or truststore";
      }
      if (anyBlank(credentials.keystorePassword(), credentials.truststorePassword())) {
        return "Security protocol " + securityProtocol.name + ": invalid password of keystore or truststore";
      }
      if (isInvalidSASLMechanism(SASLMechanism)) {
        return "Security protocol " + securityProtocol.name + ": invalid SASL mechanism, expected SCRAM-SHA-256 or SCRAM-SHA-512 got " + SASLMechanism;
      }
      if (anyBlank(credentials.SASLUsername(), credentials.SASLPassword())) {
        return "Security protocol " + securityProtocol.name + ":  invalid SASL username or password";
      }
      return null;
    }

    return "Unsupported security protocol " + securityProtocol.name;
  }

  private static boolean is(final SecurityProtocol s1, final SecurityProtocol s2) {
    return s1.name.equals(s2.name);
  }

  private static boolean is(final String s1, String s2) {
    return s1.equals(s2);
  }

  private static boolean isBlank(final String s) {
    return s == null || s.isBlank();
  }

  private static boolean isInvalidSASLMechanism(final String SASLMechanism) {
    return !(is("SCRAM-SHA-256", SASLMechanism) || is("SCRAM-SHA-512", SASLMechanism));
  }

  private static boolean anyBlank(final String... stores) {
    for (final var s : stores) {
      if (isBlank(s)) {
        return true;
      }
    }
    return false;
  }
}
