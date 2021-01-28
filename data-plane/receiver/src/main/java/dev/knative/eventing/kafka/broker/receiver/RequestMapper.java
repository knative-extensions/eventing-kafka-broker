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
package dev.knative.eventing.kafka.broker.receiver;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.security.KafkaClientsAuth;
import dev.knative.eventing.kafka.broker.core.utils.ReferenceCounter;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import io.micrometer.core.instrument.Counter;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
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
 * RequestHandler is responsible for mapping HTTP requests to Kafka records, sending records to Kafka through the Kafka
 * producer and terminating requests with the appropriate status code.
 */
public class RequestMapper implements Handler<HttpServerRequest>, IngressReconcilerListener {

  public static final int MAPPER_FAILED = BAD_REQUEST.code();
  public static final int FAILED_TO_PRODUCE = SERVICE_UNAVAILABLE.code();
  public static final int RECORD_PRODUCED = ACCEPTED.code();
  public static final int RESOURCE_NOT_FOUND = NOT_FOUND.code();

  private static final Logger logger = LoggerFactory.getLogger(RequestMapper.class);

  // ingress uuid -> IngressInfo
  // This map is used to resolve the ingress info in the reconciler listener
  private final Map<String, IngressInfo> ingressInfos;
  // producerConfig -> producer
  // This map is used to count the references to the producer instantiated for each producerConfig
  private final Map<Properties, ReferenceCounter<ProducerHolder>> producerReferences;
  // path -> IngressInfo
  // We use this map on the hot path to directly resolve the producer from the path
  private final Map<String, IngressInfo> pathMapper;

  private final Properties producerConfigs;
  private final RequestToRecordMapper requestToRecordMapper;
  private final Function<Properties, KafkaProducer<String, CloudEvent>> producerFactory;
  private final Counter badRequestCounter;
  private final Counter produceEventsCounter;
  private final Vertx vertx;
  private final AuthProvider authProvider;

  public RequestMapper(
    final Vertx vertx,
    final AuthProvider authProvider,
    final Properties producerConfigs,
    final RequestToRecordMapper requestToRecordMapper,
    final Function<Properties, KafkaProducer<String, CloudEvent>> producerFactory,
    final Counter badRequestCounter,
    final Counter produceEventsCounter) {

    Objects.requireNonNull(vertx, "provide vertx");
    Objects.requireNonNull(producerConfigs, "provide producerConfigs");
    Objects.requireNonNull(requestToRecordMapper, "provide a mapper");
    Objects.requireNonNull(producerFactory, "provide producerCreator");

    this.vertx = vertx;
    this.authProvider = authProvider;
    this.producerConfigs = producerConfigs;
    this.requestToRecordMapper = requestToRecordMapper;
    this.producerFactory = producerFactory;
    this.badRequestCounter = badRequestCounter;
    this.produceEventsCounter = produceEventsCounter;

    this.ingressInfos = new HashMap<>();
    this.producerReferences = new HashMap<>();
    this.pathMapper = new HashMap<>();
  }

  @Override
  public void handle(final HttpServerRequest request) {
    final var ingressInfo = pathMapper.get(request.path());
    if (ingressInfo == null) {
      request.response().setStatusCode(RESOURCE_NOT_FOUND).end();

      logger.warn("resource not found {} {}",
        keyValue("resources", pathMapper.keySet()),
        keyValue("path", request.path())
      );

      return;
    }

    requestToRecordMapper
      .recordFromRequest(request, ingressInfo.getTopic())
      .onSuccess(record -> ingressInfo.getProducer().send(record)
        .onSuccess(metadata -> {
          request.response().setStatusCode(RECORD_PRODUCED).end();
          produceEventsCounter.increment();

          logger.debug("Record produced {} {} {} {} {} {}",
            keyValue("topic", record.topic()),
            keyValue("partition", metadata.getPartition()),
            keyValue("offset", metadata.getOffset()),
            keyValue("value", record.value()),
            keyValue("headers", record.headers()),
            keyValue("path", request.path())
          );
        })
        .onFailure(cause -> {
          request.response().setStatusCode(FAILED_TO_PRODUCE).end();

          logger.error("Failed to send record {} {}",
            keyValue("topic", record.topic()),
            keyValue("path", request.path()),
            cause
          );
        })
      )
      .onFailure(cause -> {
        request.response().setStatusCode(MAPPER_FAILED).end();
        badRequestCounter.increment();

        logger.warn("Failed to send record {}",
          keyValue("path", request.path()),
          cause
        );
      });
  }

  @Override
  public Future<Void> onNewIngress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Ingress ingress) {

    final var producerProps = (Properties) this.producerConfigs.clone();
    if (resource.hasAuthSecret()) {
      return authProvider.getCredentials(resource.getAuthSecret().getNamespace(), resource.getAuthSecret().getName())
        .compose(credentials -> KafkaClientsAuth.updateConfigsFromProps(credentials, producerProps))
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

    // Get the rc and increment it
    final ReferenceCounter<ProducerHolder> rc = this.producerReferences.computeIfAbsent(
      producerProps,
      props -> new ReferenceCounter<>(new ProducerHolder(producerFactory.apply(props)))
    );
    rc.increment();

    final var ingressInfo = new IngressInfo(
      rc.getValue().getProducer(),
      resource.getTopics(0),
      ingress.getPath(),
      producerProps
    );

    this.pathMapper.put(ingress.getPath(), ingressInfo);
    this.ingressInfos.put(resource.getUid(), ingressInfo);

    return Future.succeededFuture();
  }

  @Override
  public Future<Void> onUpdateIngress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Ingress ingress) {
    return onDeleteIngress(resource, ingress)
      .compose(v -> onNewIngress(resource, ingress));
  }

  @Override
  public Future<Void> onDeleteIngress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Ingress ingress) {
    // Remove ingress info from the maps
    final var ingressInfo = this.ingressInfos.remove(resource.getUid());
    this.pathMapper.remove(ingressInfo.getPath());

    // Get the rc
    final var rc = this.producerReferences.get(ingressInfo.getProducerProperties());
    if (rc.decrementAndCheck()) {
      // Nobody is referring to this producer anymore, clean it up and close it
      this.producerReferences.remove(ingressInfo.getProducerProperties());
      return rc.getValue().close(vertx);
    }
    return Future.succeededFuture();
  }

  private static String encoding(final DataPlaneContract.ContentMode contentMode) {
    return switch (contentMode) {
      case BINARY -> Encoding.BINARY.toString();
      case STRUCTURED -> Encoding.STRUCTURED.toString();
      default -> throw new IllegalArgumentException("unknown content mode: " + contentMode);
    };
  }

  private static class ProducerHolder {

    private final KafkaProducer<String, CloudEvent> producer;
    private final AutoCloseable producerMeterBinder;

    ProducerHolder(final KafkaProducer<String, CloudEvent> producer) {
      this.producer = producer;
      this.producerMeterBinder = Metrics.register(producer.unwrap());
    }

    KafkaProducer<String, CloudEvent> getProducer() {
      return producer;
    }

    Future<Void> close(final Vertx vertx) {
      return producer.flush()
        .compose(
          s -> closeNow(vertx),
          c -> {
            logger.error("Failed to flush producer", c);
            return closeNow(vertx);
          }
        );
    }

    private Future<Void> closeNow(final Vertx vertx) {
      return CompositeFuture.all(
        producer.close(),
        Metrics.close(vertx, this.producerMeterBinder)
      ).mapEmpty();
    }
  }

  private static class IngressInfo {

    private final KafkaProducer<String, CloudEvent> producer;
    private final String topic;
    private final String path;
    private final Properties producerProperties;

    IngressInfo(final KafkaProducer<String, CloudEvent> producer, final String topic, final String path,
                final Properties producerProperties) {
      this.producer = producer;
      this.topic = topic;
      this.path = path;
      this.producerProperties = producerProperties;
    }

    KafkaProducer<String, CloudEvent> getProducer() {
      return producer;
    }

    String getTopic() {
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
