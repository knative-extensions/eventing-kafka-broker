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
public class RequestMapper<K, V> implements Handler<HttpServerRequest>, IngressReconcilerListener {

  public static final int MAPPER_FAILED = BAD_REQUEST.code();
  public static final int FAILED_TO_PRODUCE = SERVICE_UNAVAILABLE.code();
  public static final int RECORD_PRODUCED = ACCEPTED.code();
  public static final int RESOURCE_NOT_FOUND = NOT_FOUND.code();

  private static final Logger logger = LoggerFactory.getLogger(RequestMapper.class);

  // ingress uuid -> IngressInfo
  // This map is used to resolve the ingress info in the reconciler listener
  private final Map<String, IngressInfo<K, V>> ingressInfos;
  // producerConfig -> producer
  // This map is used to count the references to the producer instantiated for each producerConfig
  private final Map<Properties, ReferenceCounter<ProducerHolder<K, V>>> producerReferences;
  // path -> IngressInfo
  // We use this map on the hot path to directly resolve the producer from the path
  private final Map<String, IngressInfo<K, V>> pathMapper;

  private final Properties producerConfigs;
  private final RequestToRecordMapper<K, V> requestToRecordMapper;
  private final Function<Properties, KafkaProducer<K, V>> producerCreator;
  private final Counter badRequestCounter;
  private final Counter produceEventsCounter;
  private final Vertx vertx;

  /**
   * Create a new Request handler.
   *
   * @param producerConfigs       common producers configurations
   * @param requestToRecordMapper request to record mapper
   * @param producerCreator       creates a producer
   * @param badRequestCounter     count bad request responses
   * @param produceEventsCounter  count events sent to Kafka
   */
  public RequestMapper(
    final Vertx vertx,
    final Properties producerConfigs,
    final RequestToRecordMapper<K, V> requestToRecordMapper,
    final Function<Properties, KafkaProducer<K, V>> producerCreator,
    final Counter badRequestCounter,
    final Counter produceEventsCounter) {

    Objects.requireNonNull(vertx, "provide vertx");
    Objects.requireNonNull(producerConfigs, "provide producerConfigs");
    Objects.requireNonNull(requestToRecordMapper, "provide a mapper");
    Objects.requireNonNull(producerCreator, "provide producerCreator");

    this.vertx = vertx;
    this.producerConfigs = producerConfigs;
    this.requestToRecordMapper = requestToRecordMapper;
    this.producerCreator = producerCreator;
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
    // Compute the properties
    final var producerProps = (Properties) this.producerConfigs.clone();
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());
    if (ingress.getContentMode() != DataPlaneContract.ContentMode.UNRECOGNIZED) {
      producerProps.setProperty(CloudEventSerializer.ENCODING_CONFIG, encoding(ingress.getContentMode()));
    }
    producerProps.setProperty(CloudEventSerializer.EVENT_FORMAT_CONFIG, JsonFormat.CONTENT_TYPE);

    // Get the rc and increment it
    final ReferenceCounter<ProducerHolder<K, V>> rc = this.producerReferences.computeIfAbsent(
      producerProps,
      props -> new ReferenceCounter<>(new ProducerHolder<>(producerCreator.apply(props)))
    );
    rc.increment();

    final var ingressInfo = new IngressInfo<>(
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

  private static class ProducerHolder<K, V> {

    private final KafkaProducer<K, V> producer;
    private final AutoCloseable producerMeterBinder;

    ProducerHolder(final KafkaProducer<K, V> producer) {
      this.producer = producer;
      this.producerMeterBinder = Metrics.register(producer.unwrap());
    }

    KafkaProducer<K, V> getProducer() {
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

  private static class ReferenceCounter<T> {

    private final T value;
    private int refs;

    ReferenceCounter(final T value) {
      Objects.requireNonNull(value);
      this.value = value;
      this.refs = 0;
    }

    T getValue() {
      return value;
    }

    void increment() {
      this.refs++;
    }

    /**
     * @return true if the count is 0, hence nobody is referring anymore to this value
     */
    boolean decrementAndCheck() {
      this.refs--;
      return this.refs == 0;
    }
  }

  private static class IngressInfo<K, V> {

    private final KafkaProducer<K, V> producer;
    private final String topic;
    private final String path;
    private final Properties producerProperties;

    IngressInfo(final KafkaProducer<K, V> producer, final String topic, final String path,
      final Properties producerProperties) {
      this.producer = producer;
      this.topic = topic;
      this.path = path;
      this.producerProperties = producerProperties;
    }

    KafkaProducer<K, V> getProducer() {
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
