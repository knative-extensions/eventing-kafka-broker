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

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.ObjectsReconciler;
import dev.knative.eventing.kafka.broker.core.Trigger;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RequestHandler is responsible for mapping HTTP requests to Kafka records, sending records to
 * Kafka through the Kafka producer and terminating requests with the appropriate status code.
 *
 * @param <K> type of the records' key.
 * @param <V> type of the records' value.
 */
public class RequestHandler<K, V> implements Handler<HttpServerRequest>,
    ObjectsReconciler<CloudEvent> {

  public static final int MAPPER_FAILED = BAD_REQUEST.code();
  public static final int FAILED_TO_PRODUCE = SERVICE_UNAVAILABLE.code();
  public static final int RECORD_PRODUCED = ACCEPTED.code();
  public static final int BROKER_NOT_FOUND = NOT_FOUND.code();

  private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

  private final KafkaProducer<K, V> producer;
  private final RequestToRecordMapper<K, V> requestToRecordMapper;
  private final AtomicReference<Set<String>> brokers;

  /**
   * Create a new Request handler.
   *
   * @param producer              kafka producer
   * @param requestToRecordMapper request to record mapper
   */
  public RequestHandler(
      final KafkaProducer<K, V> producer,
      final RequestToRecordMapper<K, V> requestToRecordMapper) {

    Objects.requireNonNull(producer, "provide a producer");
    Objects.requireNonNull(requestToRecordMapper, "provide a mapper");

    this.producer = producer;
    this.requestToRecordMapper = requestToRecordMapper;
    brokers = new AtomicReference<>(new HashSet<>());
  }

  @Override
  public void handle(final HttpServerRequest request) {

    if (!brokers.get().contains(request.path())) {

      logger.warn("broker not found {} - brokers {}", request.path(), brokers.get());

      request.response().setStatusCode(BROKER_NOT_FOUND).end();
      return;
    }

    requestToRecordMapper
        .recordFromRequest(request)
        .onSuccess(record -> send(record)
            .onSuccess(ignore -> {
              request.response().setStatusCode(RECORD_PRODUCED).end();
              logger.debug("record produced - topic: {}", record.topic());
            })
            .onFailure(cause -> {
              request.response().setStatusCode(FAILED_TO_PRODUCE).end();
              logger.error("failed to produce - topic: {} - cause :{}", record.topic(), cause);
            })
        )
        .onFailure(cause -> {
          request.response().setStatusCode(MAPPER_FAILED).end();
          logger.warn("failed to create cloud event - path: {} - cause: {}", request.path(), cause);
        });
  }

  private Future<RecordMetadata> send(final KafkaProducerRecord<K, V> record) {
    final Promise<RecordMetadata> promise = Promise.promise();
    producer.send(record, promise);
    return promise.future();
  }

  @Override
  public Future<Void> reconcile(Map<Broker, Set<Trigger<CloudEvent>>> objects) {

    final var brokers = objects.keySet().stream()
        .map(b -> "/" + b.namespace() + "/" + b.name())
        .collect(Collectors.toSet());

    this.brokers.set(brokers);

    logger.debug("brokers: {}", brokers);

    return Future.succeededFuture();
  }
}
