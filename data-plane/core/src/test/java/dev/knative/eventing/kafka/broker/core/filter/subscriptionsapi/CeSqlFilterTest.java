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
package dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CeSqlFilterTest {

    private static final String SYSTEM_NAMESPACE = "knative-eventing";
    private static final String SECRET_NAME = "test-secret";
    private static final String RESOURCE_NAME = "test-resource";
    private static final String RESOURCE_NAMESPACE = "test-namespace";
    private static final String AES_KEY = "passphrasewhichneedstobe32bytes!"; // 32 bytes for AES-256
    private static final int GCM_NONCE_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    static final CloudEvent event = CloudEventBuilder.v1()
            .withId("123-42")
            .withDataContentType("application/cloudevents+json")
            .withDataSchema(URI.create("/api/schema"))
            .withSource(URI.create("/api/some-source"))
            .withSubject("a-subject-42")
            .withType("type")
            .withTime(OffsetDateTime.of(1985, 4, 12, 23, 20, 50, 0, ZoneOffset.UTC))
            .build();

    @ParameterizedTest
    @MethodSource(value = {"testCases"})
    public void match(CloudEvent event, String expression, boolean shouldMatch) {
        var filter = new CeSqlFilter(expression);
        assertThat(filter.test(event)).isEqualTo(shouldMatch);
    }

    static Stream<Arguments> testCases() {
        return Stream.of(
                Arguments.of(event, "'TRUE'", true),
                Arguments.of(event, "'FALSE'", false),
                Arguments.of(event, "0", false),
                Arguments.of(event, "1", true),
                Arguments.of(event, "id LIKE '123%'", true),
                Arguments.of(event, "NOT(id LIKE '123%')", false));
    }

    @BeforeAll
    static void setupCesqlRuntime() {
        Vertx vertx = mock(Vertx.class);
        KubernetesClient kubernetesClient = mock(KubernetesClient.class);
        MixedOperation secretsOperation = mock(MixedOperation.class);
        NonNamespaceOperation namespacedSecretsOperation = mock(NonNamespaceOperation.class);
        Resource<Secret> secretResource = mock(Resource.class);
        when(kubernetesClient.secrets()).thenReturn(secretsOperation);
        when(secretsOperation.inNamespace(SYSTEM_NAMESPACE)).thenReturn(namespacedSecretsOperation);
        when(namespacedSecretsOperation.withName(SECRET_NAME)).thenReturn(secretResource);
        Map<String, String> secretData = new HashMap<>();
        secretData.put(RESOURCE_NAMESPACE + "." + RESOURCE_NAME + ".key", AES_KEY);

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(SECRET_NAME)
                .withNamespace(SYSTEM_NAMESPACE)
                .endMetadata()
                .withData(secretData)
                .build();

        when(secretResource.get()).thenReturn(secret);

        // Register the function
        CeSqlRuntimeManager.getInstance().registerKnVerifyCorrelationId(vertx, kubernetesClient, SYSTEM_NAMESPACE);
    }

    @Test
    void testKnVerifyCorrelationIdFunctionRegistration() throws Exception {
        // Create test data
        String originalId = "test-correlation-id";
        int podIdx = 1;
        int replicaCount = 3;
        String encryptedData = encryptData(originalId, AES_KEY);
        String replyId = originalId + ":" + encryptedData + ":" + podIdx;

        // Create a CloudEvent with the reply ID as an extension
        CloudEvent testEvent = CloudEventBuilder.v1()
                .withId("test-id")
                .withSource(URI.create("/test"))
                .withType("test.type")
                .withExtension("knativereplyid", replyId)
                .build();

        // Test the function through CeSql filter - should return true for correct pod index
        String expression = String.format(
                "KN_VERIFY_CORRELATION_ID(knativereplyid, '%s', '%s', '%s', %d, %d)",
                RESOURCE_NAME, RESOURCE_NAMESPACE, SECRET_NAME, podIdx, replicaCount);

        CeSqlFilter filter = new CeSqlFilter(expression);
        boolean result = filter.test(testEvent);

        assertThat(result).isTrue();
    }

    @Test
    void testKnVerifyCorrelationIdFunctionWithWrongPodIndex() throws Exception {
        // Create test data with wrong pod index
        String originalId = "test-correlation-id";
        int actualPodIdx = 2;
        int expectedPodIdx = 1; // Different from actual
        int replicaCount = 3;
        String encryptedData = encryptData(originalId, AES_KEY);
        String replyId = originalId + ":" + encryptedData + ":" + actualPodIdx;

        // Create a CloudEvent with the reply ID as an extension
        CloudEvent testEvent = CloudEventBuilder.v1()
                .withId("test-id")
                .withSource(URI.create("/test"))
                .withType("test.type")
                .withExtension("knativereplyid", replyId)
                .build();

        // Test the function through CeSql filter - should return false for wrong pod index
        String expression = String.format(
                "KN_VERIFY_CORRELATION_ID(knativereplyid, '%s', '%s', '%s', %d, %d)",
                RESOURCE_NAME, RESOURCE_NAMESPACE, SECRET_NAME, expectedPodIdx, replicaCount);

        CeSqlFilter filter = new CeSqlFilter(expression);
        boolean result = filter.test(testEvent);

        assertThat(result).isFalse();
    }

    private String encryptData(String data, String key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKeySpec keySpec = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "AES");

        byte[] nonce = new byte[GCM_NONCE_LENGTH_BYTES];
        Arrays.fill(nonce, (byte) 0x01);

        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmParameterSpec);

        byte[] encryptedBytes = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
        byte[] result = new byte[nonce.length + encryptedBytes.length];
        System.arraycopy(nonce, 0, result, 0, nonce.length);
        System.arraycopy(encryptedBytes, 0, result, nonce.length, encryptedBytes.length);

        return Base64.getEncoder().encodeToString(result);
    }
}
