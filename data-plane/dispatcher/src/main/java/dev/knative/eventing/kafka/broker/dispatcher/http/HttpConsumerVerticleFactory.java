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

package dev.knative.eventing.kafka.broker.dispatcher.http;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.Trigger;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordHandler;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordSender;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class HttpConsumerVerticleFactory implements ConsumerVerticleFactory<CloudEvent> {

  private final Properties consumerConfigs;
  private final HttpClient client;
  private final Vertx vertx;
  private final Properties producerConfigs;
  private final ConsumerRecordOffsetStrategyFactory<String, CloudEvent>
      consumerRecordOffsetStrategyFactory;

  /**
   * All args constructor.
   *
   * @param consumerRecordOffsetStrategyFactory consumer offset handling strategy
   * @param consumerConfigs                     base consumer configurations.
   * @param client                              http client.
   * @param vertx                               vertx instance.
   * @param producerConfigs                     base producer configurations.
   */
  public HttpConsumerVerticleFactory(
      final ConsumerRecordOffsetStrategyFactory<String, CloudEvent>
          consumerRecordOffsetStrategyFactory,
      final Properties consumerConfigs,
      final HttpClient client,
      final Vertx vertx,
      final Properties producerConfigs) {

    Objects.requireNonNull(consumerRecordOffsetStrategyFactory,
        "provide consumerRecordOffsetStrategyFactory");
    Objects.requireNonNull(consumerConfigs, "provide consumerConfigs");
    Objects.requireNonNull(client, "provide message");
    Objects.requireNonNull(vertx, "provide vertx");
    Objects.requireNonNull(producerConfigs, "provide producerConfigs");

    this.consumerRecordOffsetStrategyFactory = consumerRecordOffsetStrategyFactory;
    this.consumerConfigs = consumerConfigs;
    this.producerConfigs = producerConfigs;
    this.client = client;
    this.vertx = vertx;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<AbstractVerticle> get(final Broker broker, final Trigger<CloudEvent> trigger) {
    Objects.requireNonNull(broker, "provide broker");
    Objects.requireNonNull(trigger, "provide trigger");

    final io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> consumer
        = createConsumer(vertx, broker, trigger);

    final io.vertx.kafka.client.producer.KafkaProducer<String, CloudEvent> producer
        = createProducer(vertx, broker, trigger);

    final CircuitBreakerOptions circuitBreakerOptions
        = createCircuitBreakerOptions(vertx, broker, trigger);

    final var triggerDestinationSender = createSender(trigger.destination(), circuitBreakerOptions);

    final ConsumerRecordSender<String, CloudEvent, HttpClientResponse> brokerDLQSender;
    if (broker.deadLetterSink() == null || broker.deadLetterSink().isEmpty()) {
      brokerDLQSender = new NoDLQSender();
    } else {
      brokerDLQSender = createSender(broker.deadLetterSink(), circuitBreakerOptions);
    }

    final var consumerOffsetManager = consumerRecordOffsetStrategyFactory
        .get(consumer, broker, trigger);

    final var sinkResponseHandler = new HttpSinkResponseHandler(broker.topic(), producer);

    final var consumerRecordHandler = new ConsumerRecordHandler<>(
        triggerDestinationSender,
        trigger.filter(),
        consumerOffsetManager,
        sinkResponseHandler,
        brokerDLQSender
    );

    return Future.succeededFuture(
        new ConsumerVerticle<>(consumer, broker.topic(), consumerRecordHandler)
    );
  }

  protected CircuitBreakerOptions createCircuitBreakerOptions(
      final Vertx vertx,
      final Broker broker,
      final Trigger<CloudEvent> trigger) {

    // TODO set circuit breaker options based on broker/trigger configurations
    return new CircuitBreakerOptions();
  }

  protected io.vertx.kafka.client.producer.KafkaProducer<String, CloudEvent> createProducer(
      final Vertx vertx,
      final Broker broker,
      final Trigger<CloudEvent> trigger) {

    // TODO check producer configurations to change per instance
    // producerConfigs is a shared object and it acts as a prototype for each consumer instance.
    final var producerConfigs = (Properties) this.producerConfigs.clone();
    producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers());

    final var kafkaProducer = new KafkaProducer<>(
        producerConfigs,
        new StringSerializer(),
        new CloudEventSerializer()
    );

    return io.vertx.kafka.client.producer.KafkaProducer.create(vertx, kafkaProducer);
  }

  protected io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> createConsumer(
      final Vertx vertx,
      final Broker broker,
      final Trigger<CloudEvent> trigger) {

    // TODO check consumer configurations to change per instance
    // consumerConfigs is a shared object and it acts as a prototype for each consumer instance.
    final var consumerConfigs = (Properties) this.consumerConfigs.clone();
    consumerConfigs.setProperty(GROUP_ID_CONFIG, trigger.id());
    consumerConfigs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers());

    // Note: KafkaConsumer instances are not thread-safe.
    // There are methods thread-safe, but in general they're not.
    final var kafkaConsumer = new KafkaConsumer<>(
        consumerConfigs,
        new StringDeserializer(),
        new CloudEventDeserializer()
    );

    return io.vertx.kafka.client.consumer.KafkaConsumer.create(vertx, kafkaConsumer);
  }

  private HttpConsumerRecordSender createSender(
      final String target,
      final CircuitBreakerOptions circuitBreakerOptions) {

    return new HttpConsumerRecordSender(
        client,
        target
    );
  }

  private static final class NoDLQSender implements
      ConsumerRecordSender<String, CloudEvent, HttpClientResponse> {

    @Override
    public Future<HttpClientResponse> send(KafkaConsumerRecord<String, CloudEvent> record) {
      final Promise<HttpClientResponse> promise = Promise.promise();
      promise.tryFail("no DLQ set");
      return promise.future();
    }
  }
}
