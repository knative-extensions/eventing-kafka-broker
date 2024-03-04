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

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenVerifierImpl implements TokenVerifier {

    private static final Logger logger = LoggerFactory.getLogger(TokenVerifierImpl.class);

    private final Vertx vertx;

    private final OIDCDiscoveryConfig oidcDiscoveryConfig;

    public TokenVerifierImpl(Vertx vertx, OIDCDiscoveryConfig oidcDiscoveryConfig) {
        this.vertx = vertx;
        this.oidcDiscoveryConfig = oidcDiscoveryConfig;
    }

    public Future<JwtClaims> verify(String token, String expectedAudience) {
        return this.vertx.<JwtClaims>executeBlocking(
                promise -> {
                    // execute blocking, as jose .process() is blocking

                    JwtConsumer jwtConsumer = new JwtConsumerBuilder()
                            .setVerificationKeyResolver(this.oidcDiscoveryConfig.getJwksVerificationKeyResolver())
                            .setExpectedAudience(expectedAudience)
                            .setExpectedIssuer(this.oidcDiscoveryConfig.getIssuer())
                            .build();

                    try {
                        JwtContext jwtContext = jwtConsumer.process(token);

                        promise.complete(jwtContext.getJwtClaims());
                    } catch (InvalidJwtException e) {
                        promise.fail(e);
                    }
                },
                false);
    }

    public Future<JwtClaims> verify(final HttpServerRequest request, String expectedAudience) {
        String authHeader = request.getHeader("Authorization");
        if (authHeader == null || authHeader.isEmpty()) {
            return Future.failedFuture("Request didn't contain Authorization header");
        }

        if (!authHeader.startsWith("Bearer ") && authHeader.length() <= "Bearer ".length()) {
            return Future.failedFuture("Authorization header didn't contain Bearer token");
        }

        String token = authHeader.substring("Bearer ".length());

        request.pause();
        return verify(token, expectedAudience).onSuccess(v -> request.resume());
    }
}
