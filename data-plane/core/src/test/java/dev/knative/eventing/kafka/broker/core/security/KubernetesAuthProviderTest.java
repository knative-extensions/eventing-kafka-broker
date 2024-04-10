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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@EnableKubernetesMockClient(https = false, crud = true)
public class KubernetesAuthProviderTest {

    KubernetesClient client;

    private AuthProvider provider;

    private static final String saslMechanismSecretKey = "sasl-mechanism";
    private static final String userCrtSecretKey = "user-crt";
    private static final String userKeySecretKey = "user-key";
    private static final String caCertificateKey = "ca-crt";
    private static final String usernameSecretKey = "username";
    private static final String userPasswordSecretKey = "password";

    @BeforeEach
    public void setUp(final Vertx vertx) {
        provider = new KubernetesAuthProvider(vertx, client);
    }

    @Test
    public void getCredentialsFromSecretSaslSsl(final Vertx vertx, final VertxTestContext context) {
        final var secretData = getSaslSslSecretData();
        final var secret = createSecret("my-secret-name", secretData);

        final var contractAuthSecretReference = DataPlaneContract.Reference.newBuilder()
                .setNamespace(secret.getMetadata().getNamespace())
                .setName(secret.getMetadata().getName())
                .build();

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setAuthSecret(contractAuthSecretReference)
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onFailure(context::failNow)
                .onSuccess(credentials -> context.verify(() -> {
                    verifyCredentials(secretData, credentials);
                    context.completeNow();
                })));
    }

    @Test
    public void getCredentialsFromAbsentAuth(final Vertx vertx, final VertxTestContext context) {
        final var secretData = getPlaintextSecretData();

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setAbsentAuth(DataPlaneContract.Empty.newBuilder().build())
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onFailure(context::failNow)
                .onSuccess(credentials -> context.verify(() -> {
                    verifyCredentials(secretData, credentials);
                    context.completeNow();
                })));
    }

    @Test
    public void getCredentialsFromMultipleSecretsPlaintext(final Vertx vertx, final VertxTestContext context) {

        final var contractMultiSecretBuilder =
                DataPlaneContract.MultiSecretReference.newBuilder().setProtocol(DataPlaneContract.Protocol.PLAINTEXT);
        final var secretData = getPlaintextSecretData();

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setMultiAuthSecret(contractMultiSecretBuilder.build())
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onFailure(context::failNow)
                .onSuccess(credentials -> context.verify(() -> {
                    verifyCredentials(secretData, credentials);
                    context.completeNow();
                })));
    }

    @Test
    public void getCredentialsFromMultipleSecretsSsl(final Vertx vertx, final VertxTestContext context) {
        final var contractMultiSecretBuilder =
                DataPlaneContract.MultiSecretReference.newBuilder().setProtocol(DataPlaneContract.Protocol.SSL);

        final Map<String, String> secretData = getSslSecretData();
        userCrtSecret(secretData, contractMultiSecretBuilder);
        caCrtSecret(secretData, contractMultiSecretBuilder);

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setMultiAuthSecret(contractMultiSecretBuilder.build())
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onFailure(context::failNow)
                .onSuccess(credentials -> context.verify(() -> {
                    verifyCredentials(secretData, credentials);
                    context.completeNow();
                })));
    }

    @Test
    public void getCredentialsFromMultipleSecretsSslNoCA(final Vertx vertx, final VertxTestContext context) {
        final var contractMultiSecretBuilder =
                DataPlaneContract.MultiSecretReference.newBuilder().setProtocol(DataPlaneContract.Protocol.SSL);

        final Map<String, String> secretData = getSslSecretData();
        secretData.remove(KubernetesCredentials.CA_CERTIFICATE_KEY);
        userCrtSecret(secretData, contractMultiSecretBuilder);

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setMultiAuthSecret(contractMultiSecretBuilder.build())
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onFailure(context::failNow)
                .onSuccess(credentials -> context.verify(() -> {
                    verifyCredentials(secretData, credentials);
                    context.completeNow();
                })));
    }

    @Test
    public void getCredentialsFromMultipleSecretsSaslSsl(final Vertx vertx, final VertxTestContext context) {
        final var contractMultiSecretBuilder =
                DataPlaneContract.MultiSecretReference.newBuilder().setProtocol(DataPlaneContract.Protocol.SASL_SSL);

        final var secretData = getSaslSslSecretData();

        saslMechanismSecret(secretData, contractMultiSecretBuilder);
        userCrtSecret(secretData, contractMultiSecretBuilder);
        usernamePasswordSecret(secretData, contractMultiSecretBuilder);
        caCrtSecret(secretData, contractMultiSecretBuilder);

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setMultiAuthSecret(contractMultiSecretBuilder.build())
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onFailure(context::failNow)
                .onSuccess(credentials -> context.verify(() -> {
                    verifyCredentials(secretData, credentials);
                    context.completeNow();
                })));
    }

    @Test
    public void getCredentialsFromMultipleSecretsSaslPlaintext(final Vertx vertx, final VertxTestContext context) {
        final var contractMultiSecretBuilder = DataPlaneContract.MultiSecretReference.newBuilder()
                .setProtocol(DataPlaneContract.Protocol.SASL_PLAINTEXT);

        final var secretData = getSaslPlaintextSecretData();

        saslMechanismSecret(secretData, contractMultiSecretBuilder);
        usernamePasswordSecret(secretData, contractMultiSecretBuilder);

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setMultiAuthSecret(contractMultiSecretBuilder.build())
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onFailure(context::failNow)
                .onSuccess(credentials -> context.verify(() -> {
                    verifyCredentials(secretData, credentials);
                    context.completeNow();
                })));
    }

    @Test
    public void shouldFailOnSecretNotFound(final Vertx vertx, final VertxTestContext context) {
        final var secretReference = DataPlaneContract.Reference.newBuilder()
                .setNamespace("my-secretReference-namespace")
                .setName("my-secretReference-name-not-found")
                .build();

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setAuthSecret(secretReference)
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onSuccess(ignored -> context.failNow("Unexpected success: expected not found error"))
                .onFailure(cause -> context.completeNow()));
    }

    @Test
    public void shouldFailOnMultiSecretNotFound(final Vertx vertx, final VertxTestContext context) {
        final var contractMultiSecretBuilder = DataPlaneContract.MultiSecretReference.newBuilder()
                .setProtocol(DataPlaneContract.Protocol.SASL_SSL)
                .addReferences(DataPlaneContract.SecretReference.newBuilder()
                        .setReference(DataPlaneContract.Reference.newBuilder()
                                .setNamespace("my-contractMultiSecretBuilder-namespace")
                                .setName("my-contractMultiSecretBuilder-name-not-found")
                                .build()))
                .build();

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setMultiAuthSecret(contractMultiSecretBuilder)
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onSuccess(ignored -> context.failNow("Unexpected success: expected not found error"))
                .onFailure(cause -> context.completeNow()));
    }

    @Test
    public void shouldFailOnInvalidSecret(final Vertx vertx, final VertxTestContext context) {

        final var data = new HashMap<String, String>();
        data.put(KubernetesCredentials.SECURITY_PROTOCOL, SecurityProtocol.SASL_SSL.name);
        data.put(KubernetesCredentials.SASL_MECHANISM, "SCRAM-SHA-512");

        final var secret = createSecret("my-secret-name-invalid", data);

        final var secretReference = DataPlaneContract.Reference.newBuilder()
                .setNamespace(secret.getMetadata().getNamespace())
                .setName(secret.getMetadata().getName())
                .build();

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setAuthSecret(secretReference)
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onSuccess(ignored -> context.failNow("Unexpected success: expected invalid secret error"))
                .onFailure(cause -> context.completeNow()));
    }

    @Test
    public void shouldFailOnInvalidMultiSecret(final Vertx vertx, final VertxTestContext context) {

        final var data = new HashMap<String, String>();
        data.put(KubernetesCredentials.SECURITY_PROTOCOL, SecurityProtocol.SASL_SSL.name);
        data.put(KubernetesCredentials.SASL_MECHANISM, "SCRAM-SHA-512");

        final var secret = createSecret("my-secret-name-invalid", data);

        final var secretReference =
                DataPlaneContract.MultiSecretReference.newBuilder().setProtocol(DataPlaneContract.Protocol.SASL_SSL);

        addSaslMechanismReference(secret, secretReference);

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setMultiAuthSecret(secretReference)
                .build();

        vertx.runOnContext(r -> provider.getCredentials(resource)
                .onSuccess(ignored -> context.failNow("Unexpected success: expected invalid secret error"))
                .onFailure(cause -> context.completeNow()));
    }

    private Secret createSecret(
            final String name,
            final Map<String, String> secretData,
            final Map<String, String> keySecretDataKeyMapping) {
        final var data = keySecretDataKeyMapping.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), secretData.get(entry.getValue())))
                .collect(Collectors.toMap(
                        AbstractMap.SimpleImmutableEntry::getKey, AbstractMap.SimpleImmutableEntry::getValue));
        return createSecret(name, data);
    }

    private Secret createSecret(final String name, final Map<String, String> data) {
        final Secret userCrtSecret = getSecret(data, name);
        client.secrets().inNamespace(userCrtSecret.getMetadata().getNamespace()).create(userCrtSecret);
        return userCrtSecret;
    }

    private static void verifyCredentials(final Map<String, String> data, Credentials credentials) {
        assertThat(credentials.SASLMechanism()).isEqualTo(data.get(KubernetesCredentials.SASL_MECHANISM));
        assertThat(credentials.securityProtocol())
                .isEqualTo(SecurityProtocol.forName(data.get(KubernetesCredentials.SECURITY_PROTOCOL)));
        assertThat(credentials.userCertificate()).isEqualTo(data.get(KubernetesCredentials.USER_CERTIFICATE_KEY));
        assertThat(credentials.userKey()).isEqualTo(data.get(KubernetesCredentials.USER_KEY_KEY));
        assertThat(credentials.caCertificates()).isEqualTo(data.get(KubernetesCredentials.CA_CERTIFICATE_KEY));
        assertThat(credentials.SASLUsername()).isEqualTo(data.get(KubernetesCredentials.USERNAME_KEY));
        assertThat(credentials.SASLPassword()).isEqualTo(data.get(KubernetesCredentials.PASSWORD_KEY));
        assertThat(credentials.skipClientAuth()).isEqualTo(data.containsKey(KubernetesCredentials.USER_SKIP_KEY));
    }

    private static Secret getSecret(final Map<String, String> data, final String name) {
        return new SecretBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace("my-secret-namespace")
                .endMetadata()
                .withData(data.entrySet().stream()
                        .map(e -> new AbstractMap.SimpleImmutableEntry<>(
                                e.getKey(),
                                Base64.getEncoder().encodeToString(e.getValue().getBytes())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .build();
    }

    private static Map<String, String> getSaslSslSecretData() {
        final var secretData = new HashMap<String, String>();
        secretData.put(KubernetesCredentials.SECURITY_PROTOCOL, SecurityProtocol.SASL_SSL.name);
        secretData.put(KubernetesCredentials.SASL_MECHANISM, "SCRAM-SHA-512");
        secretData.put(KubernetesCredentials.USER_CERTIFICATE_KEY, "my-user-cert");
        secretData.put(KubernetesCredentials.USER_KEY_KEY, "my-user-key");
        secretData.put(KubernetesCredentials.USERNAME_KEY, "my-username");
        secretData.put(KubernetesCredentials.PASSWORD_KEY, "my-user-password");
        secretData.put(KubernetesCredentials.CA_CERTIFICATE_KEY, "my-ca-certificate");
        return secretData;
    }

    private static Map<String, String> getSaslPlaintextSecretData() {
        final var secretData = new HashMap<String, String>();
        secretData.put(KubernetesCredentials.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name);
        secretData.put(KubernetesCredentials.SASL_MECHANISM, "SCRAM-SHA-512");
        secretData.put(KubernetesCredentials.USERNAME_KEY, "my-username");
        secretData.put(KubernetesCredentials.PASSWORD_KEY, "my-user-password");
        return secretData;
    }

    private static Map<String, String> getSslSecretData() {
        final var secretData = new HashMap<String, String>();
        secretData.put(KubernetesCredentials.SECURITY_PROTOCOL, SecurityProtocol.SSL.name);
        secretData.put(KubernetesCredentials.USER_CERTIFICATE_KEY, "my-user-cert");
        secretData.put(KubernetesCredentials.USER_KEY_KEY, "my-user-key");
        secretData.put(KubernetesCredentials.CA_CERTIFICATE_KEY, "my-ca-certificate");
        return secretData;
    }

    private static Map<String, String> getPlaintextSecretData() {
        final var secretData = new HashMap<String, String>();
        secretData.put(KubernetesCredentials.SECURITY_PROTOCOL, SecurityProtocol.PLAINTEXT.name);
        return secretData;
    }

    private static void addUsernamePasswordReference(
            Secret userSecret, DataPlaneContract.MultiSecretReference.Builder multiSecret) {
        multiSecret.addReferences(DataPlaneContract.SecretReference.newBuilder()
                .setReference(DataPlaneContract.Reference.newBuilder()
                        .setNamespace(userSecret.getMetadata().getNamespace())
                        .setName(userSecret.getMetadata().getName())
                        .build())
                .addKeyFieldReferences(DataPlaneContract.KeyFieldReference.newBuilder()
                        .setField(DataPlaneContract.SecretField.USER)
                        .setSecretKey(usernameSecretKey)
                        .build())
                .addKeyFieldReferences(DataPlaneContract.KeyFieldReference.newBuilder()
                        .setField(DataPlaneContract.SecretField.PASSWORD)
                        .setSecretKey(userPasswordSecretKey)
                        .build())
                .build());
    }

    private static void addSaslMechanismReference(
            Secret saslMechanismSecret, DataPlaneContract.MultiSecretReference.Builder multiSecret) {
        multiSecret.addReferences(DataPlaneContract.SecretReference.newBuilder()
                .setReference(DataPlaneContract.Reference.newBuilder()
                        .setNamespace(saslMechanismSecret.getMetadata().getNamespace())
                        .setName(saslMechanismSecret.getMetadata().getName())
                        .build())
                .addKeyFieldReferences(DataPlaneContract.KeyFieldReference.newBuilder()
                        .setField(DataPlaneContract.SecretField.SASL_MECHANISM)
                        .setSecretKey(saslMechanismSecretKey)
                        .build()));
    }

    private static void addCACertReference(
            final Secret caCert, final DataPlaneContract.MultiSecretReference.Builder multiSecret) {
        multiSecret.addReferences(DataPlaneContract.SecretReference.newBuilder()
                .setReference(DataPlaneContract.Reference.newBuilder()
                        .setNamespace(caCert.getMetadata().getNamespace())
                        .setName(caCert.getMetadata().getName())
                        .build())
                .addKeyFieldReferences(DataPlaneContract.KeyFieldReference.newBuilder()
                        .setField(DataPlaneContract.SecretField.CA_CRT)
                        .setSecretKey("ca-crt")
                        .build())
                .build());
    }

    private void addUserCertReference(
            Secret userCrtSecret, DataPlaneContract.MultiSecretReference.Builder multiSecret) {
        multiSecret.addReferences(DataPlaneContract.SecretReference.newBuilder()
                .setReference(DataPlaneContract.Reference.newBuilder()
                        .setNamespace(userCrtSecret.getMetadata().getNamespace())
                        .setName(userCrtSecret.getMetadata().getName())
                        .build())
                .addKeyFieldReferences(DataPlaneContract.KeyFieldReference.newBuilder()
                        .setField(DataPlaneContract.SecretField.USER_CRT)
                        .setSecretKey(userCrtSecretKey)
                        .build())
                .addKeyFieldReferences(DataPlaneContract.KeyFieldReference.newBuilder()
                        .setField(DataPlaneContract.SecretField.USER_KEY)
                        .setSecretKey(userKeySecretKey)
                        .build()));
    }

    private void caCrtSecret(
            Map<String, String> secretData, DataPlaneContract.MultiSecretReference.Builder multiSecret) {
        final Secret caCert = createSecret(
                "my-ca-cert", secretData, Map.of(caCertificateKey, KubernetesCredentials.CA_CERTIFICATE_KEY));
        addCACertReference(caCert, multiSecret);
    }

    private void usernamePasswordSecret(
            Map<String, String> secretData, DataPlaneContract.MultiSecretReference.Builder multiSecret) {
        final Secret userSecret = createSecret(
                "my-user-pair",
                secretData,
                Map.of(
                        usernameSecretKey, KubernetesCredentials.USERNAME_KEY,
                        userPasswordSecretKey, KubernetesCredentials.PASSWORD_KEY));
        addUsernamePasswordReference(userSecret, multiSecret);
    }

    private void userCrtSecret(
            Map<String, String> secretData, DataPlaneContract.MultiSecretReference.Builder multiSecret) {
        final Secret userCrtSecret = createSecret(
                "my-user-crt",
                secretData,
                Map.of(
                        userCrtSecretKey, KubernetesCredentials.USER_CERTIFICATE_KEY,
                        userKeySecretKey, KubernetesCredentials.USER_KEY_KEY));
        addUserCertReference(userCrtSecret, multiSecret);
    }

    private void saslMechanismSecret(
            Map<String, String> secretData, DataPlaneContract.MultiSecretReference.Builder multiSecret) {
        final Secret saslMechanismSecret = createSecret(
                "my-sasl-mechanism-name",
                secretData,
                Map.of(saslMechanismSecretKey, KubernetesCredentials.SASL_MECHANISM));
        addSaslMechanismReference(saslMechanismSecret, multiSecret);
    }
}
