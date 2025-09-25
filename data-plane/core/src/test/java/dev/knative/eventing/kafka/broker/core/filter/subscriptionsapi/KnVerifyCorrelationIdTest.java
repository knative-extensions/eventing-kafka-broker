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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.sql.EvaluationContext;
import io.cloudevents.sql.EvaluationRuntime;
import io.cloudevents.sql.Type;
import io.cloudevents.sql.impl.ExceptionFactoryImpl;
import io.cloudevents.sql.impl.runtime.EvaluationContextImpl;
import io.cloudevents.sql.impl.runtime.EvaluationResult;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.antlr.v4.runtime.misc.Interval;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class KnVerifyCorrelationIdTest {

    private static final String SYSTEM_NAMESPACE = "knative-eventing";
    private static final String SECRET_NAME = "test-secret";
    private static final String RESOURCE_NAME = "test-resource";
    private static final String RESOURCE_NAMESPACE = "test-namespace";
    private static final String AES_KEY = "passphrasewhichneedstobe32bytes!"; // 32 bytes for AES-256
    private static final String OTHER_KEY = "passphrasewhichneedstobe32bytes?"; // 32 bytes for AES-256
    private static final int GCM_NONCE_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    @Mock
    private Vertx vertx;

    @Mock
    private KubernetesClient kubernetesClient;

    @Mock
    private MixedOperation secretsOperation;

    @Mock
    private NonNamespaceOperation namespacedSecretsOperation;

    @Mock
    private Resource<Secret> secretResource;

    private KnVerifyCorrelationId function;
    private EvaluationContext evaluationContext;
    private EvaluationRuntime evaluationRuntime;
    private CloudEvent cloudEvent;

    @BeforeEach
    void setUp() {
        function = new KnVerifyCorrelationId(vertx, kubernetesClient, SYSTEM_NAMESPACE);
        evaluationContext = new EvaluationContextImpl(new Interval(0, 10), "test", new ExceptionFactoryImpl(true));
        evaluationRuntime = mock(EvaluationRuntime.class);
        cloudEvent = CloudEventBuilder.v1()
                .withId("test-id")
                .withSource(URI.create("/test"))
                .withType("test.type")
                .build();

        // Set up basic mocking structure - will be configured per test as needed
        when(kubernetesClient.secrets()).thenReturn(secretsOperation);
        when(secretsOperation.inNamespace(SYSTEM_NAMESPACE)).thenReturn(namespacedSecretsOperation);
        when(namespacedSecretsOperation.withName(SECRET_NAME)).thenReturn(secretResource);
    }

    @Test
    void testValidReplyIdWithCorrectPodIndex() throws Exception {
        String originalId = "test-correlation-id";
        int podIdx = 1;
        int replicaCount = 3;
        String encryptedData = encryptData(originalId, AES_KEY);
        String replyId = originalId + ":" + encryptedData + ":" + podIdx;

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

        List<Object> arguments =
                Arrays.asList(replyId, RESOURCE_NAME, RESOURCE_NAMESPACE, SECRET_NAME, podIdx, replicaCount);

        EvaluationResult result = function.invoke(evaluationContext, evaluationRuntime, cloudEvent, arguments);

        assertThat(result.value()).isEqualTo(true);
    }

    @Test
    void testValidReplyIdWithIncorrectPodIndex() throws Exception {
        String originalId = "test-correlation-id";
        int actualPodIdx = 2;
        int expectedPodIdx = 1;
        int replicaCount = 3;
        String encryptedData = encryptData(originalId, AES_KEY);
        String replyId = originalId + ":" + encryptedData + ":" + actualPodIdx;

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

        List<Object> arguments =
                Arrays.asList(replyId, RESOURCE_NAME, RESOURCE_NAMESPACE, SECRET_NAME, expectedPodIdx, replicaCount);

        EvaluationResult result = function.invoke(evaluationContext, evaluationRuntime, cloudEvent, arguments);

        assertThat(result.value()).isEqualTo(false);
    }

    @Test
    void testInvalidReplyIdFormat() {
        String invalidReplyId = "invalid-format";
        int podIdx = 1;
        int replicaCount = 3;

        List<Object> arguments =
                Arrays.asList(invalidReplyId, RESOURCE_NAME, RESOURCE_NAMESPACE, SECRET_NAME, podIdx, replicaCount);

        // This should throw an EvaluationException due to invalid format
        assertThatThrownBy(() -> function.invoke(evaluationContext, evaluationRuntime, cloudEvent, arguments))
                .isInstanceOf(io.cloudevents.sql.EvaluationException.class)
                .hasMessageContaining("invalid reply id");
    }

    @Test
    void testNoMatchingSecretKey() throws Exception {
        String originalId = "test-correlation-id";
        int podIdx = 1;
        int replicaCount = 3;
        String encryptedData = encryptData(originalId, AES_KEY);
        String replyId = originalId + ":" + encryptedData + ":" + podIdx;

        Map<String, String> secretData = new HashMap<>();
        secretData.put("other.resource.key", AES_KEY);

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(SECRET_NAME)
                .withNamespace(SYSTEM_NAMESPACE)
                .endMetadata()
                .withData(secretData)
                .build();

        when(secretResource.get()).thenReturn(secret);

        List<Object> arguments =
                Arrays.asList(replyId, RESOURCE_NAME, RESOURCE_NAMESPACE, SECRET_NAME, podIdx, replicaCount);

        EvaluationResult result = function.invoke(evaluationContext, evaluationRuntime, cloudEvent, arguments);

        assertThat(result.value()).isEqualTo(false);
    }

    @Test
    void testWrongEncryptionKey() throws Exception {
        String originalId = "test-correlation-id";
        int podIdx = 1;
        int replicaCount = 3;
        String encryptedData = encryptData(originalId, AES_KEY);
        String replyId = originalId + ":" + encryptedData + ":" + podIdx;

        Map<String, String> secretData = new HashMap<>();
        secretData.put(RESOURCE_NAMESPACE + "." + RESOURCE_NAME + ".key", OTHER_KEY);

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(SECRET_NAME)
                .withNamespace(SYSTEM_NAMESPACE)
                .endMetadata()
                .withData(secretData)
                .build();

        when(secretResource.get()).thenReturn(secret);

        List<Object> arguments =
                Arrays.asList(replyId, RESOURCE_NAME, RESOURCE_NAMESPACE, SECRET_NAME, podIdx, replicaCount);

        EvaluationResult result = function.invoke(evaluationContext, evaluationRuntime, cloudEvent, arguments);

        assertThat(result.value()).isEqualTo(false);
    }

    @Test
    void testFunctionMetadata() {
        assertThat(function.name()).isEqualTo("KN_VERIFY_CORRELATION_ID");
        assertThat(function.arity()).isEqualTo(6);
        assertThat(function.isVariadic()).isFalse();
        assertThat(function.returnType()).isEqualTo(Type.BOOLEAN);

        assertThat(function.typeOfParameter(0)).isEqualTo(Type.STRING);
        assertThat(function.typeOfParameter(1)).isEqualTo(Type.STRING);
        assertThat(function.typeOfParameter(2)).isEqualTo(Type.STRING);
        assertThat(function.typeOfParameter(3)).isEqualTo(Type.STRING);
        assertThat(function.typeOfParameter(4)).isEqualTo(Type.INTEGER);
        assertThat(function.typeOfParameter(5)).isEqualTo(Type.INTEGER);
    }

    @Test
    void testInvalidParameterIndex() {
        assertThatThrownBy(() -> function.typeOfParameter(6))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid parameter index");
        assertThatThrownBy(() -> function.typeOfParameter(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid parameter index");
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
