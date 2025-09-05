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
package dev.knative.eventing.kafka.broker.dispatcher.impl.auth;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dev.knative.eventing.kafka.broker.core.NamespacedName;
import io.fabric8.kubernetes.api.model.authentication.TokenRequest;
import io.fabric8.kubernetes.api.model.authentication.TokenRequestBuilder;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.vertx.VertxHttpClientFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class TokenProvider implements Closeable {

    private static final long TOKEN_EXPIRATION_SECONDS = 3600L; // 1 hour
    private static final long EXPIRATION_BUFFER_TIME_SECONDS = 300L; // 5 minutes

    private static final long CACHE_MAXIMUM_SIZE = 1000L; //  Cache up to 1000 tokens
    private static final long CACHE_EXPIRATION_TIME_SECONDS =
            TOKEN_EXPIRATION_SECONDS - EXPIRATION_BUFFER_TIME_SECONDS; // Cache tokens for 55 minutes

    private final KubernetesClient kubernetesClient;
    private final Cache<String, String> tokenCache;

    private final Vertx vertx;

    public TokenProvider(Vertx vertx) {
        this.vertx = vertx;
        this.kubernetesClient = new KubernetesClientBuilder()
                .withConfig(new ConfigBuilder().build())
                .withHttpClientFactory(new VertxHttpClientFactory(vertx))
                .build();
        this.tokenCache = CacheBuilder.newBuilder()
                .expireAfterWrite(CACHE_EXPIRATION_TIME_SECONDS, TimeUnit.SECONDS)
                .maximumSize(CACHE_MAXIMUM_SIZE)
                .initialCapacity(2)
                .build();
    }

    public Future<String> getToken(NamespacedName serviceAccount, String audience) {
        final var cacheKey = generateCacheKey(serviceAccount, audience);

        final var token = tokenCache.getIfPresent(cacheKey);
        if (token != null) {
            return Future.succeededFuture(token);
        }

        return requestToken(serviceAccount, audience).onSuccess(t -> {
            if (t != null) {
                tokenCache.put(cacheKey, t);
            }
        });
    }

    private Future<String> requestToken(NamespacedName serviceAccount, String audience) {
        return this.vertx.executeBlocking(
                promise -> {
                    try {
                        final var builder = new TokenRequestBuilder()
                                .withNewSpec()
                                .withAudiences(audience)
                                .withExpirationSeconds(TOKEN_EXPIRATION_SECONDS)
                                .endSpec()
                                .build();

                        final var tokenRequest = kubernetesClient
                                .serviceAccounts()
                                .inNamespace(serviceAccount.namespace())
                                .withName(serviceAccount.name())
                                .tokenRequest(builder);

                        if (isValidTokenRequest(tokenRequest)) {
                            promise.tryComplete(tokenRequest.getStatus().getToken());
                        } else {
                            promise.tryFail("could not request token for " + serviceAccount.name() + "/"
                                    + serviceAccount.namespace());
                        }
                    } catch (final RuntimeException exception) {
                        promise.tryFail(exception);
                    }
                },
                false);
    }

    private static boolean isValidTokenRequest(final TokenRequest tokenRequest) {
        return tokenRequest != null
                && tokenRequest.getStatus() != null
                && tokenRequest.getStatus().getToken() != null
                && !tokenRequest.getStatus().getToken().isBlank();
    }

    private String generateCacheKey(NamespacedName serviceAccount, String audience) {
        return serviceAccount.namespace() + "/" + serviceAccount.name() + "/" + audience;
    }

    @Override
    public void close() {
        if (kubernetesClient != null) {
            kubernetesClient.close();
        }
    }
}
