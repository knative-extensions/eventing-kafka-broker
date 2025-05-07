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
package dev.knative.eventing.kafka.broker.dispatcher.main;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.NamespacedName;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.filter.Filter;
import dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi.ExactFilter;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.security.Credentials;
import dev.knative.eventing.kafka.broker.core.security.KafkaClientsAuth;
import dev.knative.eventing.kafka.broker.core.tracing.kafka.ConsumerTracer;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.DeliveryOrder;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.NoopResponseHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherImpl;
import dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherMutatorChain;
import dev.knative.eventing.kafka.broker.dispatcher.impl.ResponseToHttpEndpointHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.ResponseToKafkaTopicHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventOverridesMutator;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.ConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.OffsetManager;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.OrderedConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.PartitionRevokedHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.UnorderedConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.impl.http.WebClientCloudEventSender;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

public class ConsumerVerticleBuilder {

    private static final CloudEventSender NO_DEAD_LETTER_SINK_SENDER = CloudEventSender.noop("No dead letter sink set");

    private final ConsumerVerticleContext consumerVerticleContext;

    public ConsumerVerticleBuilder(final ConsumerVerticleContext consumerVerticleContext) {
        this.consumerVerticleContext = consumerVerticleContext;
    }

    public ConsumerVerticle build() {
        return createConsumerVerticle(getInitializer());
    }

    private ConsumerVerticle.Initializer getInitializer() {
        return (vertx, consumerVerticle) -> {
            Promise<Void> promise = Promise.promise();
            consumerVerticleContext
                    .getAuthProvider()
                    .getCredentials(consumerVerticleContext.getResource())
                    .onSuccess(credentials -> {
                        try {
                            build(vertx, consumerVerticle, credentials);
                            promise.complete();
                        } catch (Exception e) {
                            promise.fail(e);
                        }
                    })
                    .onFailure(promise::fail);
            return promise.future();
        };
    }

    private void build(final Vertx vertx, final ConsumerVerticle consumerVerticle, final Credentials credentials) {
        KafkaClientsAuth.attachCredentials(consumerVerticleContext.getConsumerConfigs(), credentials);
        KafkaClientsAuth.attachCredentials(consumerVerticleContext.getProducerConfigs(), credentials);

        final ReactiveKafkaConsumer<Object, CloudEvent> consumer = this.consumerVerticleContext
                .getConsumerFactory()
                .create(vertx, consumerVerticleContext.getConsumerConfigs());
        consumerVerticle.setConsumer(consumer);

        final var metricsCloser = Metrics.register(consumer.unwrap());
        consumerVerticle.setCloser(metricsCloser);

        // setting up cloud events sender
        final var egressSubscriberSender = createConsumerRecordSender(vertx);
        final var egressDeadLetterSender = createDeadLetterSinkRecordSender(vertx);
        final var responseHandler = createResponseHandler(vertx);

        final var offsetManager = new OffsetManager(vertx, consumer, (v) -> {}, getCommitIntervalMs());

        final var recordDispatcher = new RecordDispatcherMutatorChain(
                new RecordDispatcherImpl(
                        consumerVerticleContext,
                        getFilter(),
                        egressSubscriberSender,
                        egressDeadLetterSender,
                        responseHandler,
                        offsetManager,
                        ConsumerTracer.create(
                                ((VertxInternal) vertx).tracer(),
                                consumerVerticleContext.getConsumerConfigs(),
                                TracingPolicy.PROPAGATE),
                        Metrics.getRegistry()),
                new CloudEventOverridesMutator(
                        consumerVerticleContext.getResource().getCloudEventOverrides()));
        consumerVerticle.setRecordDispatcher(recordDispatcher);

        final var partitionRevokedHandlers =
                List.of(consumerVerticle.getPartitionRevokedHandler(), offsetManager.getPartitionRevokedHandler());
        consumerVerticle.setRebalanceListener(createRebalanceListener(partitionRevokedHandlers));
    }

    private ConsumerVerticle createConsumerVerticle(final ConsumerVerticle.Initializer initializer) {
        return switch (DeliveryOrder.fromContract(
                consumerVerticleContext.getEgress().getDeliveryOrder())) {
            case ORDERED -> new OrderedConsumerVerticle(consumerVerticleContext, initializer);
            case UNORDERED -> new UnorderedConsumerVerticle(consumerVerticleContext, initializer);
        };
    }

    /**
     * For each handler call partitionRevoked and wait for the future to complete.
     *
     * @param partitionRevokedHandlers partition revoked handlers
     * @return ConsumerRebalanceListener object with the partition revoked handler running on onPartitionsRevoked
     */
    private ConsumerRebalanceListener createRebalanceListener(
            final List<PartitionRevokedHandler> partitionRevokedHandlers) {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                ConsumerVerticleContext.logger.info(
                        "Received revoke partitions for consumer {} {}",
                        consumerVerticleContext.getLoggingKeyValue(),
                        keyValue("partitions", partitions));

                final var futures = new ArrayList<Future<Void>>(partitionRevokedHandlers.size());
                for (PartitionRevokedHandler partitionRevokedHandler : partitionRevokedHandlers) {
                    futures.add(partitionRevokedHandler.partitionRevoked(partitions));
                }

                for (final var future : futures) {
                    try {
                        future.toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
                    } catch (final Exception ignored) {
                        ConsumerVerticleContext.logger.warn(
                                "Partition revoked handler failed {} {}",
                                consumerVerticleContext.getLoggingKeyValue(),
                                keyValue("partitions", partitions));
                    }
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                ConsumerVerticleContext.logger.info(
                        "Received assign partitions for consumer {} {}",
                        consumerVerticleContext.getLoggingKeyValue(),
                        keyValue("partitions", partitions));
            }
        };
    }

    private Filter getFilter() {
        // Dialected filters should override the attributes filter
        if (consumerVerticleContext.getEgress().getDialectedFilterCount() > 0) {
            return Filter.fromContract(consumerVerticleContext.getEgress().getDialectedFilterList());
        } else if (consumerVerticleContext.getEgress().hasFilter()) {
            return new ExactFilter(
                    consumerVerticleContext.getEgress().getFilter().getAttributesMap());
        }
        return Filter.noop();
    }

    private WebClientOptions createWebClientOptionsFromCACerts(final String caCerts) {
        final var pemTrustOptions = new PemTrustOptions();
        for (String trustBundle : consumerVerticleContext.getTrustBundles()) {
            pemTrustOptions.addCertValue(Buffer.buffer(trustBundle));
        }
        if (caCerts != null && !caCerts.isEmpty()) {
            pemTrustOptions.addCertValue(Buffer.buffer(caCerts));
        }
        return new WebClientOptions(consumerVerticleContext.getWebClientOptions()).setTrustOptions(pemTrustOptions);
    }

    private ResponseHandler createResponseHandler(final Vertx vertx) {
        if (consumerVerticleContext.getEgress().hasReplyUrl()) {
            var handler = new ResponseToHttpEndpointHandler(new WebClientCloudEventSender(
                    vertx,
                    WebClient.create(
                            vertx,
                            createWebClientOptionsFromCACerts(
                                    consumerVerticleContext.getEgress().getReplyUrlCACerts())),
                    consumerVerticleContext.getEgress().getReplyUrl(),
                    consumerVerticleContext.getEgress().getReplyUrlAudience(),
                    new NamespacedName(
                            consumerVerticleContext.getResource().getReference().getNamespace(),
                            consumerVerticleContext.getEgress().getOidcServiceAccountName()),
                    consumerVerticleContext,
                    Metrics.Tags.senderContext("reply")));
            if (consumerVerticleContext.getResource().hasFeatureFlags()
                    && consumerVerticleContext.getResource().getFeatureFlags().getEnableEventTypeAutocreate()) {
                return handler.withEventTypeAutocreate(
                        consumerVerticleContext.getEventTypeCreator(),
                        consumerVerticleContext.getEventTypeLister(),
                        consumerVerticleContext.getEgress().getReference());
            }
            return handler;
        }

        if (consumerVerticleContext.getEgress().hasDiscardReply()) {
            return new NoopResponseHandler();
        }

        final Properties producerConfigs = new Properties();
        producerConfigs.putAll(consumerVerticleContext.getProducerConfigs());

        final ReactiveKafkaProducer<String, CloudEvent> producer =
                this.consumerVerticleContext.getProducerFactory().create(vertx, producerConfigs);
        var handler = new ResponseToKafkaTopicHandler(
                producer, consumerVerticleContext.getResource().getTopics(0));
        if (consumerVerticleContext.getResource().hasFeatureFlags()
                && consumerVerticleContext.getResource().getFeatureFlags().getEnableEventTypeAutocreate()) {
            return handler.withEventTypeAutocreate(
                    consumerVerticleContext.getEventTypeCreator(),
                    consumerVerticleContext.getEventTypeLister(),
                    consumerVerticleContext.getEgress().getReference());
        }
        return handler;
    }

    private CloudEventSender createConsumerRecordSender(final Vertx vertx) {
        return new WebClientCloudEventSender(
                vertx,
                WebClient.create(
                        vertx,
                        createWebClientOptionsFromCACerts(
                                consumerVerticleContext.getEgress().getDestinationCACerts())),
                consumerVerticleContext.getEgress().getDestination(),
                consumerVerticleContext.getEgress().getDestinationAudience(),
                new NamespacedName(
                        consumerVerticleContext.getResource().getReference().getNamespace(),
                        consumerVerticleContext.getEgress().getOidcServiceAccountName()),
                consumerVerticleContext,
                Metrics.Tags.senderContext("subscriber"));
    }

    private CloudEventSender createDeadLetterSinkRecordSender(final Vertx vertx) {
        if (hasDeadLetterSink(consumerVerticleContext.getEgressConfig())) {
            // TODO use numPartitions for ordered delivery or max.poll.records for unordered delivery
            // if (egress.getVReplicas() > 0) {
            //   webClientOptions = new WebClientOptions(this.webClientOptions);
            //   webClientOptions.setMaxPoolSize(egress.getVReplicas());
            // }
            return new WebClientCloudEventSender(
                    vertx,
                    WebClient.create(
                            vertx,
                            createWebClientOptionsFromCACerts(
                                    consumerVerticleContext.getEgressConfig().getDeadLetterCACerts())),
                    consumerVerticleContext.getEgressConfig().getDeadLetter(),
                    consumerVerticleContext.getEgressConfig().getDeadLetterAudience(),
                    new NamespacedName(
                            consumerVerticleContext.getResource().getReference().getNamespace(),
                            consumerVerticleContext.getEgress().getOidcServiceAccountName()),
                    consumerVerticleContext,
                    Metrics.Tags.senderContext("deadlettersink"));
        }

        return NO_DEAD_LETTER_SINK_SENDER;
    }

    private int getCommitIntervalMs() {
        final var commitInterval =
                consumerVerticleContext.getConsumerConfigs().get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
        if (commitInterval == null) {
            return 5000;
        }
        return Integer.parseInt(String.valueOf(commitInterval));
    }

    private static boolean hasDeadLetterSink(final DataPlaneContract.EgressConfig egressConfig) {
        return !(egressConfig == null || egressConfig.getDeadLetter().isEmpty());
    }
}
