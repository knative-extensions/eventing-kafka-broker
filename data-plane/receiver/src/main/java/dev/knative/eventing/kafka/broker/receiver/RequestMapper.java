/*
 * Copyright 2020 The Knative Authors
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
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import io.micrometer.core.instrument.Counter;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
  // This map is used to count the references to the producer instantiated for each bootstrapServers
  private final Map<Properties, ReferenceCounter<KafkaProducer<K, V>>> producerReferences;
  // path -> IngressInfo
  // We use this map on the hot path to directly resolve the producer from the path
  private final Map<String, IngressInfo<K, V>> pathMapper;

  private final Properties producerConfigs;
  private final RequestToRecordMapper<K, V> requestToRecordMapper;
  private final Function<Properties, KafkaProducer<K, V>> producerCreator;
  private final Counter badRequestCounter;
  private final Counter produceEventsCounter;

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
    final Properties producerConfigs,
    final RequestToRecordMapper<K, V> requestToRecordMapper,
    final Function<Properties, KafkaProducer<K, V>> producerCreator,
    final Counter badRequestCounter,
    final Counter produceEventsCounter) {

    Objects.requireNonNull(producerConfigs, "provide producerConfigs");
    Objects.requireNonNull(requestToRecordMapper, "provide a mapper");
    Objects.requireNonNull(producerCreator, "provide producerCreator");

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
        .onSuccess(ignore -> {
          request.response().setStatusCode(RECORD_PRODUCED).end();
          produceEventsCounter.increment();

          logger.debug("Record produced {} {} {} {} {}",
            keyValue("topic", record.topic()),
            keyValue("partition", record.partition()),
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
    final ReferenceCounter<KafkaProducer<K, V>> rc = this
      .producerReferences
      .computeIfAbsent(producerProps, props -> new ReferenceCounter<>(producerCreator.apply(props)));
    rc.increment();

    IngressInfo<K, V> ingressInfo = new IngressInfo<>(
      rc.getValue(),
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
    var ingressInfo = this.ingressInfos.remove(resource.getUid());
    this.pathMapper.remove(ingressInfo.getPath());

    // Get the rc
    var rc = this.producerReferences.get(ingressInfo.getProducerProperties());
    if (rc.decrement()) {
      // Nobody is referring to this producer anymore, clean it up and close it
      this.producerReferences.remove(ingressInfo.getProducerProperties());
      return rc.getValue().flush().compose(v -> rc.getValue().close());
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

  private static class ReferenceCounter<T> {
    final T value;
    int count;

    public ReferenceCounter(T value) {
      this.value = value;
      this.count = 0;
    }

    public T getValue() {
      return value;
    }

    public void increment() {
      this.count++;
    }

    /**
     * @return true if the count is 0, hence nobody is referring anymore to this value
     */
    public boolean decrement() {
      this.count--;
      return this.count == 0;
    }

  }

  private static class IngressInfo<K, V> {
    final KafkaProducer<K, V> producer;
    final String topic;
    final String path;
    final Properties producerProperties;

    private IngressInfo(final KafkaProducer<K, V> producer, final String topic, String path,
                        Properties producerProperties) {
      this.producer = producer;
      this.topic = topic;
      this.path = path;
      this.producerProperties = producerProperties;
    }

    public KafkaProducer<K, V> getProducer() {
      return producer;
    }

    public String getTopic() {
      return topic;
    }

    public String getPath() {
      return path;
    }

    public Properties getProducerProperties() {
      return producerProperties;
    }
  }
}
