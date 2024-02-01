package dev.knative.eventing.kafka.broker.core.oidc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dev.knative.eventing.kafka.broker.core.NamespacedName;
import io.fabric8.kubernetes.api.model.authentication.TokenRequest;
import io.fabric8.kubernetes.api.model.authentication.TokenRequestBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.concurrent.TimeUnit;

public class TokenProvider {

    private final KubernetesClient kubernetesClient;
    private final Cache<String, String> tokenCache;

    public TokenProvider(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;

        this.tokenCache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, TimeUnit.HOURS) // 1 hour expiration after write
                .maximumSize(1000)
                .build();
    }

    public String getToken(NamespacedName serviceAccount, String audience) {
        String cacheKey = serviceAccount.namespace() + "/" + serviceAccount.name() + "/" + audience;
        String token = tokenCache.getIfPresent(cacheKey);

        if (token == null) {
            // If the token is not in the cache, request a new one
            token = requestToken(serviceAccount, audience);

            // If token is successfully retrieved, cache it
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
                .withExpirationSeconds(3600L) // 1 hour
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
}
