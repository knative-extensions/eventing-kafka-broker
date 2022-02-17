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
package dev.knative.eventing.kafka.broker.dispatcher.impl;

import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import io.cloudevents.CloudEvent;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * This class implements a {@link ResponseHandler} that will convert the sink response into a {@link CloudEvent} and push it to a Kafka topic.
 */
public final class ResponseToKafkaTopicHandler extends BaseResponseHandler implements ResponseHandler {

  private final String topic;
  private final KafkaProducer<String, CloudEvent> producer;
  private final AutoCloseable producerMeterBinder;

  /**
   * All args constructor.
   *
   * @param producer Kafka producer.
   * @param topic    topic to produce records.
   */
  public ResponseToKafkaTopicHandler(final KafkaProducer<String, CloudEvent> producer, final String topic) {
    super(LoggerFactory.getLogger(ResponseToKafkaTopicHandler.class));

    Objects.requireNonNull(topic, "provide topic");
    Objects.requireNonNull(producer, "provide producer");

    this.topic = topic;
    this.producer = producer;
    this.producerMeterBinder = Metrics.register(this.producer.unwrap());
  }

  @Override
  protected Future<Void> doHandleEvent(CloudEvent event) {
    return producer
      .send(KafkaProducerRecord.create(topic, event))
      .mapEmpty();
  }

  @Override
  public Future<Void> close() {
    return CompositeFuture.all(
      this.producer.close(),
      AsyncCloseable.wrapAutoCloseable(producerMeterBinder).close()
    ).mapEmpty();
  }
}
