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
package dev.knative.eventing.kafka.broker.core.oidc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dev.knative.eventing.kafka.broker.core.NamespacedName;
import io.fabric8.kubernetes.api.model.authentication.TokenRequest;
import io.fabric8.kubernetes.api.model.authentication.TokenRequestBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.util.concurrent.TimeUnit;

public class TokenProvider {

    private static final long TOKEN_EXPIRATION_SECONDS = 3600L; // 1 hour
    private static final long EXPIRATION_BUFFER_TIME_SECONDS = 300L; // 5 minutes

    private static final long CACHE_MAXIMUM_SIZE = 1000L; //  Cache up to 1000 tokens
    private static final long CACHE_EXPIRATION_TIME_SECONDS =
            TOKEN_EXPIRATION_SECONDS - EXPIRATION_BUFFER_TIME_SECONDS; // Cache tokens for 55 minutes

    private final KubernetesClient kubernetesClient;
    private final Cache<String, String> tokenCache;

    public TokenProvider() {
        Config clientConfig = new ConfigBuilder().build();
        kubernetesClient =
                new KubernetesClientBuilder().withConfig(clientConfig).build();

        this.tokenCache = CacheBuilder.newBuilder()
                .expireAfterWrite(CACHE_EXPIRATION_TIME_SECONDS, TimeUnit.SECONDS)
                .maximumSize(CACHE_MAXIMUM_SIZE)
                .build();
    }

    public String getToken(NamespacedName serviceAccount, String audience) {
        String cacheKey = generateCacheKey(serviceAccount, audience);
        String token = tokenCache.getIfPresent(cacheKey);

        if (token == null) {
            token = requestToken(serviceAccount, audience);
            if (token != null) {
                tokenCache.put(cacheKey, token);
            }
        }

        return token;
    }

    private String requestToken(NamespacedName serviceAccount, String audience) {
        TokenRequest tokenRequest = new TokenRequestBuilder()
                .withNewSpec()
                .withAudiences(audience)
                .withExpirationSeconds(TOKEN_EXPIRATION_SECONDS)
                .endSpec()
                .build();

        tokenRequest = kubernetesClient
                .serviceAccounts()
                .inNamespace(serviceAccount.namespace())
                .withName(serviceAccount.name())
                .tokenRequest(tokenRequest);

        if (tokenRequest != null && tokenRequest.getStatus() != null) {
            return tokenRequest.getStatus().getToken();
        } else {
            return null;
        }
    }

    private String generateCacheKey(NamespacedName serviceAccount, String audience) {
        return serviceAccount.namespace() + "/" + serviceAccount.name() + "/" + audience;
    }
}
