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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.keys.resolvers.JwksVerificationKeyResolver;
import org.jose4j.lang.JoseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OIDCDiscoveryConfig {

    private static final Logger logger = LoggerFactory.getLogger(TokenVerifier.class);

    private static final String OIDC_DISCOVERY_URL = "https://kubernetes.default.svc/.well-known/openid-configuration";

    private String issuer;

    private JwksVerificationKeyResolver jwksVerificationKeyResolver;

    private OIDCDiscoveryConfig() {}

    public String getIssuer() {
        return issuer;
    }

    public JwksVerificationKeyResolver getJwksVerificationKeyResolver() {
        return jwksVerificationKeyResolver;
    }

    public static Future<OIDCDiscoveryConfig> build(Vertx vertx) {
        Config kubeConfig = new ConfigBuilder().build();

        WebClientOptions webClientOptions = new WebClientOptions()
                .setPemTrustOptions(new PemTrustOptions().addCertPath(kubeConfig.getCaCertFile()));
        WebClient webClient = WebClient.create(vertx, webClientOptions);

        OIDCDiscoveryConfig oidcDiscoveryConfig = new OIDCDiscoveryConfig();

        return webClient
                .getAbs(OIDC_DISCOVERY_URL)
                .bearerTokenAuthentication(kubeConfig.getAutoOAuthToken())
                .send()
                .compose(res -> {
                    logger.debug("Got raw OIDC discovery info: " + res.bodyAsString());

                    try {
                        if (res.statusCode() != 200) {
                            return Future.failedFuture("Unexpected status (" + res.statusCode()
                                    + ") on OIDC discovery endpoint: " + res.bodyAsString());
                        }

                        ObjectMapper mapper = new ObjectMapper();
                        OIDCInfo oidcInfo = mapper.readValue(res.bodyAsString(), OIDCInfo.class);

                        oidcDiscoveryConfig.issuer = oidcInfo.getIssuer();

                        return webClient
                                .getAbs(oidcInfo.getJwks().toString())
                                .bearerTokenAuthentication(kubeConfig.getAutoOAuthToken())
                                .send();

                    } catch (JsonProcessingException e) {
                        logger.error("Failed to parse OIDC discovery info", e);

                        return Future.failedFuture(e);
                    }
                })
                .compose(res -> {
                    if (res.statusCode() >= 200 && res.statusCode() < 300) {
                        try {
                            JsonWebKeySet jsonWebKeySet = new JsonWebKeySet(res.bodyAsString());
                            logger.debug("Got JWKeys: " + jsonWebKeySet.toJson());

                            oidcDiscoveryConfig.jwksVerificationKeyResolver =
                                    new JwksVerificationKeyResolver(jsonWebKeySet.getJsonWebKeys());

                            return Future.succeededFuture(oidcDiscoveryConfig);
                        } catch (JoseException t) {
                            logger.error("Failed to parse JWKeys", t);

                            return Future.failedFuture(t);
                        }
                    }

                    logger.error("Got unexpected response code for JWKey URL: " + res.statusCode());

                    return Future.failedFuture("unexpected response code on JWKeys URL: " + res.statusCode());
                });
    }
}
