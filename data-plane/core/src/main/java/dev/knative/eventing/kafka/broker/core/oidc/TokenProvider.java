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

import dev.knative.eventing.kafka.broker.core.NamespacedName;
import io.fabric8.kubernetes.api.model.authentication.TokenRequest;
import io.fabric8.kubernetes.api.model.authentication.TokenRequestBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

public class TokenProvider {

    private final KubernetesClient kubernetesClient;

    public TokenProvider() {
        Config clientConfig = new ConfigBuilder().build();

        kubernetesClient =
                new KubernetesClientBuilder().withConfig(clientConfig).build();
    }

    public String requestToken(NamespacedName serviceAccount, String audience) {
        TokenRequest tokenRequest = new TokenRequestBuilder()
                .withNewSpec()
                .withAudiences(audience)
                .withExpirationSeconds(3600L)
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
