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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;

public class CredentialsTest {

    /**
     * A minimal {@link Credentials} implementation that does not override the
     * OAUTHBEARER passthrough default methods, exercising their default (null) behaviour.
     */
    private static final class MinimalCredentials implements Credentials {
        @Override
        public String caCertificates() {
            return null;
        }

        @Override
        public String userCertificate() {
            return null;
        }

        @Override
        public String userKey() {
            return null;
        }

        @Override
        public String SASLMechanism() {
            return null;
        }

        @Override
        public String SASLUsername() {
            return null;
        }

        @Override
        public String SASLPassword() {
            return null;
        }

        @Override
        public SecurityProtocol securityProtocol() {
            return null;
        }

        @Override
        public boolean skipClientAuth() {
            return false;
        }
    }

    @Test
    public void defaultSASLJaasConfigReturnsNull() {
        assertThat(new MinimalCredentials().SASLJaasConfig()).isNull();
    }

    @Test
    public void defaultSASLLoginCallbackHandlerClassReturnsNull() {
        assertThat(new MinimalCredentials().SASLLoginCallbackHandlerClass()).isNull();
    }
}
