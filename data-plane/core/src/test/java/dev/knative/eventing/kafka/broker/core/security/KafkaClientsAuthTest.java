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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Properties;
import javax.security.auth.spi.LoginModule;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.junit.jupiter.api.Test;

public class KafkaClientsAuthTest {

    @Test
    public void shouldConfigureSaslScram512Ssl() {
        shouldConfigureSaslSsl(ScramLoginModule.class, "SCRAM-SHA-512");
    }

    @Test
    public void shouldConfigureSaslScram256Ssl() {
        shouldConfigureSaslSsl(ScramLoginModule.class, "SCRAM-SHA-256");
    }

    @Test
    public void shouldConfigureSaslPlainSsl() {
        shouldConfigureSaslSsl(PlainLoginModule.class, "PLAIN");
    }

    @Test
    public void shouldConfigureSaslDefaultedPlainSsl() {
        shouldConfigureSaslSsl(PlainLoginModule.class, null);
    }

    private static void shouldConfigureSaslSsl(final Class<? extends LoginModule> module, final String mechanism) {
        final var props = new Properties();

        final var credentials = mock(Credentials.class);
        when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
        when(credentials.caCertificates()).thenReturn("xyz");
        if (mechanism != null) {
            when(credentials.SASLMechanism()).thenReturn(mechanism);
        }
        when(credentials.SASLUsername()).thenReturn("aaa");
        when(credentials.SASLPassword()).thenReturn("bbb");

        assertThatCode(() -> KafkaClientsAuth.attachCredentials(props, credentials))
                .doesNotThrowAnyException();

        final var expected = new Properties();
        expected.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());
        expected.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
        expected.setProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "xyz");
        if (mechanism != null) {
            expected.setProperty(SaslConfigs.SASL_MECHANISM, mechanism);
        } else {
            expected.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN"); // it is defaulted
        }
        expected.setProperty(
                SaslConfigs.SASL_JAAS_CONFIG,
                module.getName() + " required username=\"" + credentials.SASLUsername() + "\" password=\""
                        + credentials.SASLPassword() + "\";");

        assertThat(props).isEqualTo(expected);

        final var producerConfigs = new HashMap<String, Object>();
        final var consumerConfigs = new HashMap<String, Object>();

        assertThatCode(() -> KafkaClientsAuth.attachCredentials(producerConfigs, credentials))
                .doesNotThrowAnyException();
        assertThatCode(() -> KafkaClientsAuth.attachCredentials(consumerConfigs, credentials))
                .doesNotThrowAnyException();

        assertThat(producerConfigs).isEqualTo(expected);
        assertThat(consumerConfigs).isEqualTo(expected);
    }

    @Test
    public void shouldConfigureSsl() {
        final var props = new Properties();

        final var credentials = mock(Credentials.class);
        when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SSL);
        when(credentials.userCertificate()).thenReturn("abc");
        when(credentials.userKey()).thenReturn("key");
        when(credentials.caCertificates()).thenReturn("xyz");

        assertThatCode(() -> KafkaClientsAuth.attachCredentials(props, credentials))
                .doesNotThrowAnyException();

        final var expected = new Properties();
        expected.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
        expected.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
        expected.setProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "xyz");
        expected.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
        expected.setProperty(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "abc");
        expected.setProperty(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, "key");

        assertThat(props).isEqualTo(expected);

        final var producerConfigs = new HashMap<String, Object>();
        final var consumerConfigs = new HashMap<String, Object>();

        assertThatCode(() -> KafkaClientsAuth.attachCredentials(producerConfigs, credentials))
                .doesNotThrowAnyException();
        assertThatCode(() -> KafkaClientsAuth.attachCredentials(consumerConfigs, credentials))
                .doesNotThrowAnyException();

        assertThat(producerConfigs).isEqualTo(expected);
        assertThat(consumerConfigs).isEqualTo(expected);
    }

    @Test
    public void shouldConfigureSaslPlaintextScram512() {
        shouldConfigureSaslPlaintext("SCRAM-SHA-512");
    }

    @Test
    public void shouldConfigureSaslPlaintextScram256() {
        shouldConfigureSaslPlaintext("SCRAM-SHA-256");
    }

    @Test
    public void shouldConfigurePlaintext() {
        final var props = new Properties();

        final var credentials = mock(Credentials.class);
        when(credentials.securityProtocol()).thenReturn(SecurityProtocol.PLAINTEXT);

        assertThatCode(() -> KafkaClientsAuth.attachCredentials(props, credentials))
                .doesNotThrowAnyException();

        final var expected = new Properties();
        expected.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());

        assertThat(props).isEqualTo(expected);

        final var producerConfigs = new HashMap<String, Object>();
        final var consumerConfigs = new HashMap<String, Object>();

        assertThatCode(() -> KafkaClientsAuth.attachCredentials(producerConfigs, credentials))
                .doesNotThrowAnyException();
        assertThatCode(() -> KafkaClientsAuth.attachCredentials(consumerConfigs, credentials))
                .doesNotThrowAnyException();

        assertThat(producerConfigs).isEqualTo(expected);
        assertThat(consumerConfigs).isEqualTo(expected);
    }

    private static void shouldConfigureSaslPlaintext(final String mechanism) {
        final var props = new Properties();

        final var credentials = mock(Credentials.class);
        when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SASL_PLAINTEXT);
        when(credentials.SASLMechanism()).thenReturn(mechanism);
        when(credentials.SASLUsername()).thenReturn("aaa");
        when(credentials.SASLPassword()).thenReturn("bbb");

        assertThatCode(() -> KafkaClientsAuth.attachCredentials(props, credentials))
                .doesNotThrowAnyException();

        final var expected = new Properties();

        expected.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        expected.setProperty(SaslConfigs.SASL_MECHANISM, mechanism);
        expected.setProperty(
                SaslConfigs.SASL_JAAS_CONFIG,
                ScramLoginModule.class.getName() + " required username=\"" + credentials.SASLUsername()
                        + "\" password=\"" + credentials.SASLPassword() + "\";");

        assertThat(props).isEqualTo(expected);

        final var producerConfigs = new HashMap<String, Object>();
        final var consumerConfigs = new HashMap<String, Object>();

        assertThatCode(() -> KafkaClientsAuth.attachCredentials(producerConfigs, credentials))
                .doesNotThrowAnyException();
        assertThatCode(() -> KafkaClientsAuth.attachCredentials(consumerConfigs, credentials))
                .doesNotThrowAnyException();

        assertThat(producerConfigs).isEqualTo(expected);
        assertThat(consumerConfigs).isEqualTo(expected);
    }

    // --- OAUTHBEARER tests ---

    @Test
    public void shouldConfigureOauthbearerWithBothKeys() {
        final var props = new Properties();

        final var jaasConfig = OAuthBearerLoginModule.class.getName()
                + " required scope=\"https://example.servicebus.windows.net/.default\";";
        final var handlerClass = "io.conduktor.kafka.security.oauthbearer.azure.AzureManagedIdentityCallbackHandler";

        final var credentials = mock(Credentials.class);
        when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
        when(credentials.SASLMechanism()).thenReturn("OAUTHBEARER");
        when(credentials.SASLJaasConfig()).thenReturn(jaasConfig);
        when(credentials.SASLLoginCallbackHandlerClass()).thenReturn(handlerClass);
        when(credentials.caCertificates()).thenReturn(null);

        KafkaClientsAuth.attachCredentials(props, credentials);

        assertThat(props.getProperty(SaslConfigs.SASL_MECHANISM)).isEqualTo("OAUTHBEARER");
        assertThat(props.getProperty(SaslConfigs.SASL_JAAS_CONFIG)).isEqualTo(jaasConfig);
        assertThat(props.getProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS))
                .isEqualTo(handlerClass);
        assertThat(props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG))
                .isEqualTo(SecurityProtocol.SASL_SSL.name());
    }

    @Test
    public void shouldConfigureOauthbearerWithNeitherKey_mskIamRegression() {
        // When neither sasl.jaas.config nor sasl.login.callback.handler.class is set,
        // the properties must be identical to the previous behaviour (empty block).
        // This preserves AWS MSK IAM which relies on its own classpath-provided login module.
        final var props = new Properties();

        final var credentials = mock(Credentials.class);
        when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
        when(credentials.SASLMechanism()).thenReturn("OAUTHBEARER");
        when(credentials.SASLJaasConfig()).thenReturn(null);
        when(credentials.SASLLoginCallbackHandlerClass()).thenReturn(null);
        when(credentials.caCertificates()).thenReturn(null);

        KafkaClientsAuth.attachCredentials(props, credentials);

        assertThat(props.getProperty(SaslConfigs.SASL_MECHANISM)).isEqualTo("OAUTHBEARER");
        assertThat(props).doesNotContainKey(SaslConfigs.SASL_JAAS_CONFIG);
        assertThat(props).doesNotContainKey(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
    }

    @Test
    public void shouldConfigureOauthbearerWithOnlyHandlerClass() {
        final var props = new Properties();
        final var handlerClass = "com.example.MyOAuthHandler";

        final var credentials = mock(Credentials.class);
        when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
        when(credentials.SASLMechanism()).thenReturn("OAUTHBEARER");
        when(credentials.SASLJaasConfig()).thenReturn(null);
        when(credentials.SASLLoginCallbackHandlerClass()).thenReturn(handlerClass);
        when(credentials.caCertificates()).thenReturn(null);

        KafkaClientsAuth.attachCredentials(props, credentials);

        assertThat(props.getProperty(SaslConfigs.SASL_MECHANISM)).isEqualTo("OAUTHBEARER");
        assertThat(props).doesNotContainKey(SaslConfigs.SASL_JAAS_CONFIG);
        assertThat(props.getProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS))
                .isEqualTo(handlerClass);
    }

    @Test
    public void shouldIgnoreHandlerClassOnNonOauthbearerMechanism() {
        // If sasl.login.callback.handler.class is set but mechanism is SCRAM, warn and ignore.
        final var props = new Properties();

        final var credentials = mock(Credentials.class);
        when(credentials.securityProtocol()).thenReturn(SecurityProtocol.SASL_SSL);
        when(credentials.SASLMechanism()).thenReturn("SCRAM-SHA-512");
        when(credentials.SASLUsername()).thenReturn("user");
        when(credentials.SASLPassword()).thenReturn("pass");
        when(credentials.SASLLoginCallbackHandlerClass()).thenReturn("com.example.Handler");
        when(credentials.caCertificates()).thenReturn(null);

        KafkaClientsAuth.attachCredentials(props, credentials);

        // Handler class must NOT be set
        assertThat(props).doesNotContainKey(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        // Normal SCRAM config must still work
        assertThat(props.getProperty(SaslConfigs.SASL_MECHANISM)).isEqualTo("SCRAM-SHA-512");
        assertThat(props.getProperty(SaslConfigs.SASL_JAAS_CONFIG)).contains("ScramLoginModule");
    }
}
