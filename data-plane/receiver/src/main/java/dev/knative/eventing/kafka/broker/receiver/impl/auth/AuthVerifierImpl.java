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
package dev.knative.eventing.kafka.broker.receiver.impl.auth;

import dev.knative.eventing.kafka.broker.core.features.FeaturesConfig;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthVerifierImpl implements AuthVerifier {

    private static final Logger logger = LoggerFactory.getLogger(AuthVerifierImpl.class);

    private Vertx vertx;
    private final OIDCDiscoveryConfigListener oidcDiscoveryConfigListener;

    private OIDCDiscoveryConfig oidcDiscoveryConfig;
    private int callbackId;

    public AuthVerifierImpl(OIDCDiscoveryConfigListener oidcDiscoveryConfigListener) {
        this.oidcDiscoveryConfigListener = oidcDiscoveryConfigListener;
    }

    public void start(Vertx vertx) {
        this.vertx = vertx;

        oidcDiscoveryConfig = oidcDiscoveryConfigListener.getOidcDiscoveryConfig();

        callbackId = oidcDiscoveryConfigListener.registerCallback(config -> {
            this.oidcDiscoveryConfig = config;
        });
    }

    public void stop() {
        oidcDiscoveryConfigListener.deregisterCallback(callbackId);
    }

    private Future<JwtClaims> verifyAuthN(String token, IngressProducer ingressInfo) {
        return this.vertx.<JwtClaims>executeBlocking(
                promise -> {
                    // execute blocking, as jose .process() is blocking

                    if (oidcDiscoveryConfig == null) {
                        promise.fail(
                                "OIDC discovery config not initialized. This is most likely the case when the pod was started with an invalid OIDC config in place and then later the "
                                        + FeaturesConfig.KEY_AUTHENTICATION_OIDC
                                        + " flag was enabled. Restarting the pod should help.");
                    }

                    JwtConsumer jwtConsumer = new JwtConsumerBuilder()
                            .setVerificationKeyResolver(this.oidcDiscoveryConfig.getJwksVerificationKeyResolver())
                            .setExpectedAudience(ingressInfo.getAudience())
                            .setExpectedIssuer(this.oidcDiscoveryConfig.getIssuer())
                            .build();

                    try {
                        JwtContext jwtContext = jwtConsumer.process(token);

                        promise.complete(jwtContext.getJwtClaims());
                    } catch (InvalidJwtException e) {
                        promise.fail(new AuthenticationException(e));
                    }
                },
                false);
    }

    private Future<JwtClaims> verifyAuthN(final HttpServerRequest request, IngressProducer ingressInfo) {
        String authHeader = request.getHeader("Authorization");
        if (authHeader == null || authHeader.isEmpty()) {
            return Future.failedFuture(new AuthenticationException("Request didn't contain Authorization header"));
        }

        if (!authHeader.startsWith("Bearer ") && authHeader.length() <= "Bearer ".length()) {
            return Future.failedFuture(new AuthenticationException("Authorization header didn't contain Bearer token"));
        }

        String token = authHeader.substring("Bearer ".length());

        request.pause();
        return verifyAuthN(token, ingressInfo).onSuccess(v -> request.resume());
    }

    private Future<CloudEvent> verifyAuthZ(HttpServerRequest request, JwtClaims claims, IngressProducer ingressInfo) {
        // claims from Map<String, List<Object>> to Map<String, List<String>>
        var convertedClaims = claims.flattenClaims().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        v -> v.getValue().stream().map(Object::toString).toList()));

        // first we check, if we have EventPolicies, which matches on the claims
        final var claimMatchingPolicies = new ArrayList<EventPolicy>(0);
        for (EventPolicy ep : ingressInfo.getEventPolicies()) {
            if (ep.matchesClaims(convertedClaims)) {
                claimMatchingPolicies.add(ep);
            }
        }

        if (claimMatchingPolicies.isEmpty()) {
            return Future.failedFuture(new AuthorizationException("Not authorized by any EventPolicy"));
        }

        // in case we have Policies which matches on the claims, we check on the filters too,
        // so we need to read the cloudevent from the request only, when the "basic" authz check succeeded.
        return VertxMessageFactory.createReader(request)
                .map(MessageReader::toEvent)
                .compose(cloudEvent -> {
                    for (EventPolicy ep : claimMatchingPolicies) {
                        if (ep.matchesCloudEvent(cloudEvent)) {
                            // as soon as one policy matches, we're good
                            return Future.succeededFuture(cloudEvent);
                        }
                    }

                    return Future.failedFuture(new AuthorizationException("Not authorized by any EventPolicy"));
                });
    }

    public Future<CloudEvent> verify(final HttpServerRequest request, IngressProducer ingressInfo) {
        return verifyAuthN(request, ingressInfo).compose(jwtClaims -> verifyAuthZ(request, jwtClaims, ingressInfo));
    }
}
