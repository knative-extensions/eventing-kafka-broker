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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.EgressConfig;
import dev.knative.eventing.kafka.broker.core.wrappers.Egress;
import dev.knative.eventing.kafka.broker.core.wrappers.Resource;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordHandler;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordSender;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpConsumerVerticleFactory implements ConsumerVerticleFactory {

  private static final Logger logger = LoggerFactory.getLogger(HttpConsumerVerticleFactory.class);

  private final static ConsumerRecordSender<String, CloudEvent, HttpResponse<Buffer>> NO_DLQ_SENDER =
    record -> Future.failedFuture("no DLQ set");

  private final Properties consumerConfigs;
  private final WebClient client;
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
    final WebClient client,
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
  public Future<AbstractVerticle> get(final Resource resource, final Egress egress) {
    Objects.requireNonNull(resource, "provide resource");
    Objects.requireNonNull(egress, "provide egress");

    final io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> consumer
      = createConsumer(vertx, resource, egress);

    final io.vertx.kafka.client.producer.KafkaProducer<String, CloudEvent> producer
      = createProducer(vertx, resource, egress);

    final CircuitBreakerOptions circuitBreakerOptions = createCircuitBreakerOptions(resource);

    final var egressDestinationSender = createSender(
      egress.destination(),
      circuitBreakerOptions,
      resource.egressConfig()
    );

    final var egressConfig = resource.egressConfig();
    final ConsumerRecordSender<String, CloudEvent, HttpResponse<Buffer>> egressDeadLetterSender =
      egressConfig == null || egressConfig.getDeadLetter() == null || egressConfig.getDeadLetter().isEmpty()
        ? NO_DLQ_SENDER
        : createSender(resource.egressConfig().getDeadLetter(), circuitBreakerOptions, resource.egressConfig());

    final var consumerOffsetManager = consumerRecordOffsetStrategyFactory
      .get(consumer, resource, egress);

    final var sinkResponseHandler = new HttpSinkResponseHandler(resource.topics().iterator().next(), producer);

    final var consumerRecordHandler = new ConsumerRecordHandler<>(
      egressDestinationSender,
      egress.filter(),
      consumerOffsetManager,
      sinkResponseHandler,
      egressDeadLetterSender
    );

    return Future.succeededFuture(
      new ConsumerVerticle<>(consumer, resource.topics(), consumerRecordHandler)
    );
  }

  private static CircuitBreakerOptions createCircuitBreakerOptions(final Resource resource) {

    final var config = resource.egressConfig();
    if (config == null) {
      return new CircuitBreakerOptions();
    }

    return new CircuitBreakerOptions()
      .setMaxRetries(config.getRetry());
  }

  protected io.vertx.kafka.client.producer.KafkaProducer<String, CloudEvent> createProducer(
    final Vertx vertx,
    final Resource resource,
    final Egress egress) {

    // producerConfigs is a shared object and it acts as a prototype for each consumer instance.
    final var producerConfigs = (Properties) this.producerConfigs.clone();
    producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.bootstrapServers());

    final var kafkaProducer = new KafkaProducer<>(
      producerConfigs,
      new StringSerializer(),
      new CloudEventSerializer()
    );

    return io.vertx.kafka.client.producer.KafkaProducer.create(vertx, kafkaProducer);
  }

  protected io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> createConsumer(
    final Vertx vertx,
    final Resource resource,
    final Egress egress) {

    // consumerConfigs is a shared object and it acts as a prototype for each consumer instance.
    final var consumerConfigs = (Properties) this.consumerConfigs.clone();
    consumerConfigs.setProperty(GROUP_ID_CONFIG, egress.consumerGroup());
    consumerConfigs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.bootstrapServers());

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
    final CircuitBreakerOptions circuitBreakerOptions,
    final EgressConfig egress) {

    final var circuitBreaker = CircuitBreaker.create(target, vertx, circuitBreakerOptions);
    circuitBreaker.retryPolicy(computeRetryPolicy(egress));

    return new HttpConsumerRecordSender(
      client,
      target,
      circuitBreaker
    );
  }

  private static Function<Integer, Long> computeRetryPolicy(EgressConfig egress) {
    if (egress != null && egress.getBackoffPolicy() != null) {
      try {
        final var delay = Duration.parse(egress.getBackoffDelay()).toMillis();

        return switch (egress.getBackoffPolicy()) {
          case Linear -> retryCount -> linearRetryPolicy(retryCount, delay);
          case Exponential, UNRECOGNIZED -> retryCount -> exponentialRetryPolicy(retryCount, delay);
        };

      } catch (final Exception ex) {
        logger.error("failed to set retry policy", ex);
      }
    }
    return retry -> 0L; // Default Vert.x retry policy
  }

  private static Long exponentialRetryPolicy(final int retryCount, final long delay) {
    return delay * Math.round(Math.pow(2, retryCount));
  }

  private static Long linearRetryPolicy(final int retryCount, final long delay) {
    return delay * retryCount;
  }
}
