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

import dev.knative.eventing.kafka.broker.core.features.FeaturesConfig;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import io.vertx.core.Vertx;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OIDCDiscoveryConfigListener implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(OIDCDiscoveryConfigListener.class);
    private final String featuresConfigPath;
    private final Vertx vertx;
    private final FileWatcher configFeaturesWatcher;
    private final int timeoutSeconds;
    private final CopyOnWriteArrayList<Consumer<OIDCDiscoveryConfig>> callbacks;
    private final AtomicReference<OIDCDiscoveryConfig> oidcDiscoveryConfig;

    public OIDCDiscoveryConfigListener(String featuresConfigPath, Vertx vertx, int timeoutSeconds) throws IOException {
        this.featuresConfigPath = featuresConfigPath;
        this.vertx = vertx;
        this.timeoutSeconds = timeoutSeconds;
        this.oidcDiscoveryConfig = new AtomicReference<>();
        this.callbacks = new CopyOnWriteArrayList<>();

        this.buildFeaturesAndOIDCDiscoveryConfig();

        this.configFeaturesWatcher =
                new FileWatcher(new File(featuresConfigPath + "/" + FeaturesConfig.KEY_AUTHENTICATION_OIDC), () -> {
                    if (this.oidcDiscoveryConfig.get() == null) {
                        this.buildFeaturesAndOIDCDiscoveryConfig();
                        OIDCDiscoveryConfig config = this.oidcDiscoveryConfig.get();
                        if (config != null) {
                            this.callbacks.forEach(callback -> callback.accept(config));
                        }
                    }
                });

        this.configFeaturesWatcher.start();
    }

    public OIDCDiscoveryConfig getOidcDiscoveryConfig() {
        return oidcDiscoveryConfig.get();
    }

    public int registerCallback(Consumer<OIDCDiscoveryConfig> callback) {
        this.callbacks.add(callback);
        return this.callbacks.size() - 1;
    }

    public void deregisterCallback(int callbackId) {
        this.callbacks.set(callbackId, null);
    }

    private void buildOIDCDiscoveryConfig() throws ExecutionException, InterruptedException, TimeoutException {
        OIDCDiscoveryConfig config = OIDCDiscoveryConfig.build(this.vertx)
                .toCompletionStage()
                .toCompletableFuture()
                .get(this.timeoutSeconds, TimeUnit.SECONDS);
        this.oidcDiscoveryConfig.set(config);
    }

    private void buildFeaturesAndOIDCDiscoveryConfig() {
        try {
            FeaturesConfig featuresConfig = new FeaturesConfig(featuresConfigPath);
            if (featuresConfig.isAuthenticationOIDC()) {
                try {
                    this.buildOIDCDiscoveryConfig();
                } catch (ExecutionException | InterruptedException | TimeoutException e) {
                    logger.error("Unable to build OIDC Discover Config even though OIDC authentication is enabled", e);
                }
            }
        } catch (IOException e) {
            logger.warn("failed to get feature config, skipping building OIDC Discovery Config", e);
        }
    }

    @Override
    public void close() throws Exception {
        this.configFeaturesWatcher.close();
    }
}
