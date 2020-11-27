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
package dev.knative.eventing.kafka.broker.dispatcher.http;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.EgressConfig;
import dev.knative.eventing.kafka.broker.core.filter.Filter;
import dev.knative.eventing.kafka.broker.core.filter.impl.AttributesFilter;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordHandler;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategy;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordSender;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public class HttpConsumerVerticleFactory implements ConsumerVerticleFactory {

  private final static ConsumerRecordSender<String, CloudEvent, HttpResponse<Buffer>> NO_DLQ_SENDER =
    record -> Future.failedFuture("no DLQ set");

  private final Properties consumerConfigs;
  private final WebClientOptions webClientOptions;
  private final Properties producerConfigs;
  private final ConsumerRecordOffsetStrategyFactory<String, CloudEvent> consumerRecordOffsetStrategyFactory;

  /**
   * All args constructor.
   *
   * @param consumerRecordOffsetStrategyFactory consumer offset handling strategy
   * @param consumerConfigs                     base consumer configurations.
   * @param webClientOptions                    web client options.
   * @param producerConfigs                     base producer configurations.
   */
  public HttpConsumerVerticleFactory(
    final ConsumerRecordOffsetStrategyFactory<String, CloudEvent> consumerRecordOffsetStrategyFactory,
    final Properties consumerConfigs,
    final WebClientOptions webClientOptions,
    final Properties producerConfigs) {

    Objects.requireNonNull(consumerRecordOffsetStrategyFactory, "provide consumerRecordOffsetStrategyFactory");
    Objects.requireNonNull(consumerConfigs, "provide consumerConfigs");
    Objects.requireNonNull(webClientOptions, "provide webClientOptions");
    Objects.requireNonNull(producerConfigs, "provide producerConfigs");

    this.consumerRecordOffsetStrategyFactory = consumerRecordOffsetStrategyFactory;
    this.consumerConfigs = consumerConfigs;
    this.producerConfigs = producerConfigs;
    this.webClientOptions = webClientOptions;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AbstractVerticle get(final DataPlaneContract.Resource resource,
    final DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource, "provide resource");
    Objects.requireNonNull(egress, "provide egress");

    final var consumer = createConsumer(resource, egress);
    final var producer = createProducer(resource, egress);

    final CircuitBreakerOptions circuitBreakerOptions = createCircuitBreakerOptions(resource);

    final var egressConfig = resource.getEgressConfig();

    final var egressDestinationSender = createSender(
      egress.getDestination(),
      circuitBreakerOptions,
      egressConfig
    );

    final Function<Vertx, ConsumerRecordSender<String, CloudEvent, HttpResponse<Buffer>>> egressDeadLetterSender =
      egressConfig == null || egressConfig.getDeadLetter() == null || egressConfig.getDeadLetter().isEmpty()
        ? vertx -> NO_DLQ_SENDER
        : createSender(egressConfig.getDeadLetter(), circuitBreakerOptions, egressConfig);

    final Function<KafkaConsumer<String, CloudEvent>, ConsumerRecordOffsetStrategy<String, CloudEvent>>
      consumerOffsetManager = c -> consumerRecordOffsetStrategyFactory.get(c, resource, egress);

    final BiFunction<Vertx, KafkaProducer<String, CloudEvent>, HttpSinkResponseHandler> sinkResponseHandler =
      (vertx, p) -> new HttpSinkResponseHandler(vertx, resource.getTopics(0), p);

    final BiFunction<Vertx, KafkaConsumer<String, CloudEvent>, Handler<KafkaConsumerRecord<String, CloudEvent>>> consumerRecordHandler =
      (vertx, c) -> {

        final KafkaProducer<String, CloudEvent> p = producer.apply(vertx);

        return new ConsumerRecordHandler<>(
          egressDestinationSender.apply(vertx),
          (egress.hasFilter()) ? new AttributesFilter(egress.getFilter().getAttributesMap()) : Filter.noop(),
          consumerOffsetManager.apply(c),
          sinkResponseHandler.apply(vertx, p),
          egressDeadLetterSender.apply(vertx)
        );
      };

    return new ConsumerVerticle<>(
      consumer,
      new HashSet<>(resource.getTopicsList()),
      consumerRecordHandler
    );
  }

  private static CircuitBreakerOptions createCircuitBreakerOptions(final DataPlaneContract.Resource resource) {
    if (resource.hasEgressConfig()) {
      return new CircuitBreakerOptions()
        .setMaxRetries(resource.getEgressConfig().getRetry());
    }

    return new CircuitBreakerOptions();
  }

  protected Function<Vertx, KafkaProducer<String, CloudEvent>> createProducer(
    final DataPlaneContract.Resource resource,
    final DataPlaneContract.Egress egress) {

    // producerConfigs is a shared object and it acts as a prototype for each consumer instance.
    final var producerConfigs = this.producerConfigs.entrySet()
      .stream()
      .map(e -> new SimpleImmutableEntry<>(e.getKey().toString(), e.getValue().toString()))
      .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    // TODO create a single producer per bootstrap servers.
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());

    return vertx -> KafkaProducer.create(vertx, producerConfigs);
  }

  protected Function<Vertx, KafkaConsumer<String, CloudEvent>> createConsumer(
    final DataPlaneContract.Resource resource,
    final DataPlaneContract.Egress egress) {

    // this.consumerConfigs is a shared object and it acts as a prototype for each consumer instance.
    final var consumerConfigs = this.consumerConfigs.entrySet()
      .stream()
      .map(e -> new SimpleImmutableEntry<>(e.getKey().toString(), e.getValue()))
      .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    consumerConfigs.put(GROUP_ID_CONFIG, egress.getConsumerGroup());
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());

    final var opt = new KafkaClientOptions()
      .setConfig(consumerConfigs)
      .setTracingPolicy(TracingPolicy.PROPAGATE);

    return vertx -> KafkaConsumer.create(vertx, opt);
  }

  private Function<Vertx, ConsumerRecordSender<String, CloudEvent, HttpResponse<Buffer>>> createSender(
    final String target,
    final CircuitBreakerOptions circuitBreakerOptions,
    final EgressConfig egress) {

    return vertx -> {
      final var circuitBreaker = CircuitBreaker.create(target, vertx, circuitBreakerOptions);
      circuitBreaker.retryPolicy(computeRetryPolicy(egress));

      return new HttpConsumerRecordSender(
        vertx,
        target,
        circuitBreaker,
        WebClient.create(vertx, this.webClientOptions)
      );
    };
  }

  /* package visibility for test */
  static Function<Integer, Long> computeRetryPolicy(final EgressConfig egress) {
    if (egress != null && egress.getBackoffPolicy() != null && egress.getBackoffDelay() > 0) {
      final var delay = egress.getBackoffDelay();
      return switch (egress.getBackoffPolicy()) {
        case Linear -> retryCount -> linearRetryPolicy(retryCount, delay);
        case Exponential, UNRECOGNIZED -> retryCount -> exponentialRetryPolicy(retryCount, delay);
      };
    }
    return retry -> 0L; // Default Vert.x retry policy, it means don't retry
  }

  private static Long exponentialRetryPolicy(final int retryCount, final long delay) {
    return delay * Math.round(Math.pow(2, retryCount));
  }

  private static Long linearRetryPolicy(final int retryCount, final long delay) {
    return delay * retryCount;
  }
}
