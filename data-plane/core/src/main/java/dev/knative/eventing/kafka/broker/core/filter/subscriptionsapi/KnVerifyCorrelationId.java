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

import io.cloudevents.CloudEvent;
import io.cloudevents.sql.EvaluationContext;
import io.cloudevents.sql.EvaluationRuntime;
import io.cloudevents.sql.Type;
import io.cloudevents.sql.impl.functions.BaseFunction;
import io.cloudevents.sql.impl.runtime.EvaluationResult;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.Vertx;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnVerifyCorrelationId extends BaseFunction {
    private static final Logger logger = LoggerFactory.getLogger(KnVerifyCorrelationId.class);
    private final Vertx vertx;
    private final KubernetesClient kubeClient;
    private final String systemNamespace;
    private static final int GCM_NONCE_LENGTH_BYTES =
            12; // This aligns with the default nonce length used in the go cypher package
    private static final int GCM_TAG_LENGTH_BITS =
            128; // this aligns with the default tag length used in the go cypher package

    public KnVerifyCorrelationId(Vertx vertx, KubernetesClient kubeClient, String systemNamespace) {
        super("KN_VERIFY_CORRELATION_ID");
        this.vertx = vertx;
        this.kubeClient = kubeClient;
        this.systemNamespace = systemNamespace;
    }

    @Override
    public EvaluationResult invoke(
            EvaluationContext ctx, EvaluationRuntime evaluationRuntime, CloudEvent cloudEvent, List<Object> arguments) {
        var result = false;
        try {
            final String replyId = (String) arguments.get(0);
            final String name = (String) arguments.get(1);
            final String namespace = (String) arguments.get(2);
            final String secretName = (String) arguments.get(3);
            final Integer podIdx = (Integer) arguments.get(4);
            final Integer replicaCount = (Integer) arguments.get(5);

            final var actualIdx = getPodIdxFromReplyId(replyId);
            if (actualIdx % replicaCount != podIdx) {
                return new EvaluationResult(false);
            }

            var secret = getSecret(secretName, systemNamespace);
            result = secret.getData().entrySet().stream()
                    .filter((k) -> keyIsForResource(k.getKey(), name, namespace))
                    .anyMatch((k) -> this.isValidReplyId(replyId, k.getValue()));
        } catch (Exception e) {
            return new EvaluationResult(
                    result,
                    ctx.exceptionFactory()
                            .functionExecutionError(name(), e)
                            .create(ctx.expressionInterval(), ctx.expressionText()));
        }

        return new EvaluationResult(result);
    }

    @Override
    public Type typeOfParameter(int i) throws IllegalArgumentException {
        return switch (i) {
            case 0, 1, 2, 3 -> Type.STRING;
            case 4, 5 -> Type.INTEGER;
            default -> throw new IllegalArgumentException("invalid parameter index");
        };
    }

    @Override
    public Type returnType() {
        return Type.BOOLEAN;
    }

    @Override
    public int arity() {
        return 6;
    }

    @Override
    public boolean isVariadic() {
        return false;
    }

    private Secret getSecret(String name, String namespace) {
        return kubeClient.secrets().inNamespace(namespace).withName(name).get();
    }

    private int getPodIdxFromReplyId(String replyId) throws IllegalArgumentException {
        var parts = replyId.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("invalid reply id");
        }

        return Integer.parseInt(parts[2]);
    }

    private boolean keyIsForResource(String key, String name, String namespace) {
        return key.startsWith(namespace + "." + name + ".");
    }

    private boolean isValidReplyId(String replyId, String aesKey) {
        List<String> parts = Arrays.asList(replyId.split(":"));
        String originalId = parts.get(0);
        String encryptedIdBase64 = parts.get(1);

        try {
            byte[] encryptedBytes = Base64.getDecoder().decode(encryptedIdBase64);
            if (encryptedBytes.length < GCM_NONCE_LENGTH_BYTES) {
                throw new IllegalArgumentException("encrypted data is too short");
            }

            byte[] nonceBytes = Arrays.copyOfRange(encryptedBytes, 0, GCM_NONCE_LENGTH_BYTES);
            byte[] cipherText = Arrays.copyOfRange(encryptedBytes, GCM_NONCE_LENGTH_BYTES, encryptedBytes.length);

            final Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            SecretKeySpec keySpec = new SecretKeySpec(aesKey.getBytes(StandardCharsets.UTF_8), "AES");
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonceBytes);

            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmParameterSpec);

            byte[] decryptedBytes = cipher.doFinal(cipherText);

            String decryptedId = new String(decryptedBytes, StandardCharsets.UTF_8);
            return decryptedId.equals(originalId);
        } catch (Exception e) {
            logger.error("Failed to verify KN_VERIFY_CORRELATION_ID", e);
            return false;
        }
    }
}
