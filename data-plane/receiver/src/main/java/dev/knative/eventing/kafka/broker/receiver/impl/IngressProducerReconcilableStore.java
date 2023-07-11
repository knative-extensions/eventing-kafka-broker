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
package dev.knative.eventing.kafka.broker.receiver.impl;

import com.google.common.base.Strings;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.security.KafkaClientsAuth;
import dev.knative.eventing.kafka.broker.core.utils.ReferenceCounter;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Future;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a store of {@link IngressProducer} that can be reconciled
 * using it as {@link IngressReconcilerListener} of a {@link dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler}.
 * <p>
 * Because of its state associated to the specific verticles, this class cannot be shared among verticles.
 */
public class IngressProducerReconcilableStore implements IngressReconcilerListener {

    private final Properties producerConfigs;
    private final Function<Properties, ReactiveKafkaProducer<String, CloudEvent>> producerFactory;
    private final AuthProvider authProvider;

    // ingress uuid -> IngressInfo
    // This map is used to resolve the ingress info in the reconciler listener
    private final Map<String, IngressProducerImpl> ingressInfos;
    // producerConfig -> producer
    // This map is used to count the references to the producer instantiated for each producerConfig
    private final Map<Properties, ReferenceCounter<ProducerHolder>> producerReferences;
    // path -> IngressInfo
    // We use this map on the hot path to directly resolve the producer from the path
    private final Map<String, IngressProducerImpl> pathMapper;
    // host -> IngressInfo
    // We use this map on the root path to directly resolve the producer from the hostname
    private final Map<String, IngressProducerImpl> hostMapper;

    public IngressProducerReconcilableStore(
            final AuthProvider authProvider,
            final Properties producerConfigs,
            final Function<Properties, ReactiveKafkaProducer<String, CloudEvent>> producerFactory) {

        Objects.requireNonNull(producerConfigs, "provide producerConfigs");
        Objects.requireNonNull(producerFactory, "provide producerCreator");

        this.authProvider = authProvider;
        this.producerConfigs = producerConfigs;
        this.producerFactory = producerFactory;

        this.ingressInfos = new HashMap<>();
        this.producerReferences = new HashMap<>();
        this.pathMapper = new HashMap<>();
        this.hostMapper = new HashMap<>();
    }

    public IngressProducer resolve(String host, String path) {
        // Ignore the host when there's a path given in the request.
        // That means, we support these modes:
        // - Request coming to "/path" --> path is used for matching
        // - Request coming to "/" --> hostname is used for matching
        final var p = pathMapper.get(removeTrailingSlash(path));
        if (p != null) {
            return p;
        }

        // Host based routing is not as simple as path based routing, since the host header
        // is variable.
        // For example an external name service might be hit in multiple ways producing
        // different host headers:
        // - my-kafka-channel-kn-channel.default
        // - my-kafka-channel-kn-channel.default:80
        // - my-kafka-channel-kn-channel.default.svc
        // - my-kafka-channel-kn-channel.default.svc:80
        // - my-kafka-channel-kn-channel.default.svc.cluster.local [1]
        // - my-kafka-channel-kn-channel.default.svc.cluster.local:80
        //
        // Current implementations expects only the long form that contains the cluster
        // domain without the port [1], however, this is clearly a non-robust implementation,
        // or it might be wrong with some clients, for now, we keep the same behavior as
        // the old implementations.

        return hostMapper.get(host);
    }

    private String removeTrailingSlash(final String path) {
        if (path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    @Override
    public Future<Void> onNewIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress) {
        if (this.ingressInfos.containsKey(resource.getUid())) {
            return Future.succeededFuture();
        }

        // Compute the properties
        final var producerProps = (Properties) this.producerConfigs.clone();

        return authProvider
                .getCredentials(resource)
                .map(credentials -> KafkaClientsAuth.attachCredentials(producerProps, credentials))
                .compose(configs -> onNewIngress(resource, ingress, configs));
    }

    private Future<Void> onNewIngress(
            final DataPlaneContract.Resource resource,
            final DataPlaneContract.Ingress ingress,
            final Properties producerProps) {
        // Compute the properties.
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());
        if (ingress.getContentMode() != DataPlaneContract.ContentMode.UNRECOGNIZED) {
            producerProps.setProperty(CloudEventSerializer.ENCODING_CONFIG, encoding(ingress.getContentMode()));
        }
        producerProps.setProperty(CloudEventSerializer.EVENT_FORMAT_CONFIG, JsonFormat.CONTENT_TYPE);

        try {
            // Get the rc and increment it
            final ReferenceCounter<ProducerHolder> rc =
                    this.producerReferences.computeIfAbsent(producerProps, props -> {
                        final var producer = producerFactory.apply(producerProps);
                        return new ReferenceCounter<>(new ProducerHolder(producer));
                    });
            rc.increment();

            final var ingressInfo = new IngressProducerImpl(
                    rc.getValue().getProducer(), resource, ingress.getPath(), ingress.getHost(), producerProps);

            if (isRootPath(ingress.getPath()) && Strings.isNullOrEmpty(ingress.getHost())) {
                throw new IllegalArgumentException(
                        "Ingress path and host is blank. One of them should be defined. Resource UID: "
                                + resource.getUid());
            }

            if (!isRootPath(ingress.getPath())) {
                this.pathMapper.put(ingress.getPath(), ingressInfo);
            }
            if (!Strings.isNullOrEmpty(ingress.getHost())) {
                this.hostMapper.put(ingress.getHost(), ingressInfo);
            }
            this.ingressInfos.put(resource.getUid(), ingressInfo);

            return Future.succeededFuture();

        } catch (final Exception ex) {
            return Future.failedFuture(ex);
        }
    }

    @Override
    public Future<Void> onUpdateIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress) {
        // TODO this update can produce errors when onDeleteIngress finishes and before onNewIngress creates mappings.
        return onDeleteIngress(resource, ingress).compose(v -> onNewIngress(resource, ingress));
    }

    @Override
    public Future<Void> onDeleteIngress(
            final DataPlaneContract.Resource resource, final DataPlaneContract.Ingress ingress) {
        if (!this.ingressInfos.containsKey(resource.getUid())) {
            return Future.succeededFuture();
        }

        final var ingressInfo = this.ingressInfos.get(resource.getUid());

        // Get the rc
        final var rc = this.producerReferences.get(ingressInfo.getProducerProperties());
        if (rc.decrementAndCheck()) {
            // Nobody is referring to this producer anymore, clean it up and close it
            this.producerReferences.remove(ingressInfo.getProducerProperties());
            return rc.getValue().close().onSuccess(r -> {
                // Remove ingress info from the maps
                if (!isRootPath(ingressInfo.getPath())) {
                    this.pathMapper.remove(ingressInfo.getPath());
                }
                if (!Strings.isNullOrEmpty(ingressInfo.getHost())) {
                    this.hostMapper.remove(ingressInfo.getHost());
                }
                this.ingressInfos.remove(resource.getUid());
            });
        }
        // Remove ingress info from the maps
        if (!isRootPath(ingressInfo.getPath())) {
            this.pathMapper.remove(ingressInfo.getPath());
        }
        if (!Strings.isNullOrEmpty(ingressInfo.getHost())) {
            this.hostMapper.remove(ingressInfo.getHost());
        }
        this.ingressInfos.remove(resource.getUid());

        return Future.succeededFuture();
    }

    private static String encoding(final DataPlaneContract.ContentMode contentMode) {
        return switch (contentMode) {
            case BINARY -> Encoding.BINARY.toString();
            case STRUCTURED -> Encoding.STRUCTURED.toString();
            default -> throw new IllegalArgumentException("unknown content mode: " + contentMode);
        };
    }

    private static class ProducerHolder implements AsyncCloseable {

        private static final Logger logger = LoggerFactory.getLogger(ProducerHolder.class);

        private final ReactiveKafkaProducer<String, CloudEvent> producer;
        private final AsyncCloseable producerMeterBinder;

        ProducerHolder(final ReactiveKafkaProducer<String, CloudEvent> producer) {
            this.producer = producer;
            this.producerMeterBinder = Metrics.register(producer.unwrap());
        }

        ReactiveKafkaProducer<String, CloudEvent> getProducer() {
            return producer;
        }

        @Override
        public Future<Void> close() {
            return producer.flush().compose(s -> closeNow(), c -> {
                logger.error("Failed to flush producer", c);
                return closeNow();
            });
        }

        private Future<Void> closeNow() {
            return AsyncCloseable.compose(this.producerMeterBinder, producer::close)
                    .close();
        }
    }

    private static class IngressProducerImpl implements IngressProducer {

        private final ReactiveKafkaProducer<String, CloudEvent> producer;
        private final String topic;
        private final String path;
        private final String host;
        private final Properties producerProperties;
        private final DataPlaneContract.Reference reference;

        IngressProducerImpl(
                final ReactiveKafkaProducer<String, CloudEvent> producer,
                final DataPlaneContract.Resource resource,
                final String path,
                final String host,
                final Properties producerProperties) {
            this.producer = producer;
            this.topic = resource.getTopics(0);
            this.reference = resource.getReference();
            this.path = path;
            this.host = host;
            this.producerProperties = producerProperties;
        }

        @Override
        public ReactiveKafkaProducer<String, CloudEvent> getKafkaProducer() {
            return producer;
        }

        @Override
        public String getTopic() {
            return topic;
        }

        @Override
        public DataPlaneContract.Reference getReference() {
            return reference;
        }

        String getPath() {
            return path;
        }

        String getHost() {
            return host;
        }

        Properties getProducerProperties() {
            return producerProperties;
        }
    }

    private boolean isRootPath(String path) {
        return Strings.isNullOrEmpty(path) || path.equals("/");
    }
}
