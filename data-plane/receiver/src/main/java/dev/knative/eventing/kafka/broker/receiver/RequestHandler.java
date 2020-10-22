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
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RequestHandler is responsible for mapping HTTP requests to Kafka records, sending records to Kafka through the Kafka
 * producer and terminating requests with the appropriate status code.
 */
public class RequestHandler<K, V> implements Handler<HttpServerRequest>, IngressReconcilerListener {

  public static final int MAPPER_FAILED = BAD_REQUEST.code();
  public static final int FAILED_TO_PRODUCE = SERVICE_UNAVAILABLE.code();
  public static final int RECORD_PRODUCED = ACCEPTED.code();
  public static final int RESOURCE_NOT_FOUND = NOT_FOUND.code();

  private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

  private final RequestToRecordMapper<K, V> requestToRecordMapper;
  // path -> <bootstrapServers, producer>
  private final AtomicReference<Map<String, Entry<String, Producer<K, V>>>> producers;
  private final Properties producerConfigs;
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
  public RequestHandler(
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
    producers = new AtomicReference<>(new HashMap<>());
    this.badRequestCounter = badRequestCounter;
    this.produceEventsCounter = produceEventsCounter;
  }

  @Override
  public void handle(final HttpServerRequest request) {
    final var producer = producers.get().get(request.path());
    if (producer == null) {
      request.response().setStatusCode(RESOURCE_NOT_FOUND).end();

      logger.warn("resource not found {} {}",
        keyValue("resources", producers.get().keySet()),
        keyValue("path", request.path())
      );

      return;
    }

    requestToRecordMapper
      .recordFromRequest(request, producer.getValue().topic)
      .onSuccess(record -> producer.getValue().producer.send(record)
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
    final var producerConfigs = (Properties) this.producerConfigs.clone();
    producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());
    if (ingress.getContentMode() != DataPlaneContract.ContentMode.UNRECOGNIZED) {
      producerConfigs.setProperty(CloudEventSerializer.ENCODING_CONFIG, encoding(ingress.getContentMode()));
    }
    producerConfigs.setProperty(CloudEventSerializer.EVENT_FORMAT_CONFIG, JsonFormat.CONTENT_TYPE);

    final KafkaProducer<K, V> producer = producerCreator.apply(producerConfigs);

    this.producers.get().put(
      ingress.getPath(),
      new SimpleImmutableEntry<>(
        resource.getBootstrapServers(),
        new Producer<>(producer, resource.getTopics(0))
      )
    );

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
    var producer = this.producers.get().remove(ingress.getPath())
      .getValue().producer;
    return producer
      .flush()
      .compose(v -> producer.close());
  }

  private static String encoding(final DataPlaneContract.ContentMode contentMode) {
    return switch (contentMode) {
      case BINARY -> Encoding.BINARY.toString();
      case STRUCTURED -> Encoding.STRUCTURED.toString();
      default -> throw new IllegalArgumentException("unknown content mode: " + contentMode);
    };
  }

  private static class Producer<K, V> {

    final KafkaProducer<K, V> producer;
    final String topic;

    private Producer(final KafkaProducer<K, V> producer, final String topic) {
      this.producer = producer;
      this.topic = topic;
    }
  }
}
