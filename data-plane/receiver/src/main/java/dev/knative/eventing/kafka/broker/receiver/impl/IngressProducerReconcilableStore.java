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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.security.KafkaClientsAuth;
import dev.knative.eventing.kafka.broker.core.utils.ReferenceCounter;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import dev.knative.eventing.kafka.broker.receiver.IngressProducerStore;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an implementation of {@link IngressProducerStore} that can be reconciled
 * using it as {@link IngressReconcilerListener} of a {@link dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler}.
 * <p>
 * Because of its state associated to the specific verticles, this class cannot be shared among verticles.
 */
public class IngressProducerReconcilableStore implements IngressProducerStore, IngressReconcilerListener {

  private final Properties producerConfigs;
  private final Function<Properties, KafkaProducer<String, CloudEvent>> producerFactory;
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

  public IngressProducerReconcilableStore(
    final AuthProvider authProvider,
    final Properties producerConfigs,
    final Function<Properties, KafkaProducer<String, CloudEvent>> producerFactory
  ) {

    Objects.requireNonNull(producerConfigs, "provide producerConfigs");
    Objects.requireNonNull(producerFactory, "provide producerCreator");

    this.authProvider = authProvider;
    this.producerConfigs = producerConfigs;
    this.producerFactory = producerFactory;

    this.ingressInfos = new HashMap<>();
    this.producerReferences = new HashMap<>();
    this.pathMapper = new HashMap<>();
  }

  @Override
  public IngressProducer resolve(String path) {
    return pathMapper.get(path);
  }

  @Override
  public Future<Void> onNewIngress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Ingress ingress) {
    if (this.ingressInfos.containsKey(resource.getUid())) {
      return Future.succeededFuture();
    }

    // Compute the properties
    final var producerProps = (Properties) this.producerConfigs.clone();
    if (resource.hasAuthSecret()) {
      return authProvider.getCredentials(resource.getAuthSecret().getNamespace(), resource.getAuthSecret().getName())
        .map(credentials -> KafkaClientsAuth.attachCredentials(producerProps, credentials))
        .compose(configs -> onNewIngress(resource, ingress, configs));
    }
    return onNewIngress(resource, ingress, producerProps);
  }

  private Future<Void> onNewIngress(final DataPlaneContract.Resource resource,
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
      final ReferenceCounter<ProducerHolder> rc = this.producerReferences.computeIfAbsent(producerProps, props -> {
        final var producer = producerFactory.apply(producerProps);
        return new ReferenceCounter<>(new ProducerHolder(producer));
      });
      rc.increment();

      final var ingressInfo = new IngressProducerImpl(
        rc.getValue().getProducer(),
        resource.getTopics(0),
        ingress.getPath(),
        producerProps
      );

      this.pathMapper.put(ingress.getPath(), ingressInfo);
      this.ingressInfos.put(resource.getUid(), ingressInfo);

      return Future.succeededFuture();

    } catch (final Exception ex) {
      return Future.failedFuture(ex);
    }
  }

  @Override
  public Future<Void> onUpdateIngress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Ingress ingress) {
    // TODO this update can produce errors when onDeleteIngress finishes and before onNewIngress creates mappings.
    return onDeleteIngress(resource, ingress)
      .compose(v -> onNewIngress(resource, ingress));
  }

  @Override
  public Future<Void> onDeleteIngress(
    final DataPlaneContract.Resource resource,
    final DataPlaneContract.Ingress ingress) {
    if (!this.ingressInfos.containsKey(resource.getUid())) {
      return Future.succeededFuture();
    }

    final var ingressInfo = this.ingressInfos.get(resource.getUid());

    // Get the rc
    final var rc = this.producerReferences.get(ingressInfo.getProducerProperties());
    if (rc.decrementAndCheck()) {
      // Nobody is referring to this producer anymore, clean it up and close it
      this.producerReferences.remove(ingressInfo.getProducerProperties());
      return rc.getValue().close()
        .onSuccess(r -> {
          // Remove ingress info from the maps
          this.pathMapper.remove(ingressInfo.getPath());
          this.ingressInfos.remove(resource.getUid());
        });
    }
    // Remove ingress info from the maps
    this.pathMapper.remove(ingressInfo.getPath());
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

    private final KafkaProducer<String, CloudEvent> producer;
    private final AutoCloseable producerMeterBinder;

    ProducerHolder(final KafkaProducer<String, CloudEvent> producer) {
      this.producer = producer;
      this.producerMeterBinder = Metrics.register(producer.unwrap());
    }

    KafkaProducer<String, CloudEvent> getProducer() {
      return producer;
    }

    @Override
    public Future<Void> close() {
      return producer.flush()
        .compose(
          s -> closeNow(),
          c -> {
            logger.error("Failed to flush producer", c);
            return closeNow();
          }
        );
    }

    private Future<Void> closeNow() {
      return AsyncCloseable.compose(
        producer::close,
        AsyncCloseable.wrapAutoCloseable(this.producerMeterBinder)
      ).close();
    }
  }

  private static class IngressProducerImpl implements IngressProducer {

    private final KafkaProducer<String, CloudEvent> producer;
    private final String topic;
    private final String path;
    private final Properties producerProperties;

    IngressProducerImpl(final KafkaProducer<String, CloudEvent> producer, final String topic, final String path,
                        final Properties producerProperties) {
      this.producer = producer;
      this.topic = topic;
      this.path = path;
      this.producerProperties = producerProperties;
    }

    @Override
    public KafkaProducer<String, CloudEvent> getKafkaProducer() {
      return producer;
    }

    @Override
    public String getTopic() {
      return topic;
    }

    String getPath() {
      return path;
    }

    Properties getProducerProperties() {
      return producerProperties;
    }
  }

}
