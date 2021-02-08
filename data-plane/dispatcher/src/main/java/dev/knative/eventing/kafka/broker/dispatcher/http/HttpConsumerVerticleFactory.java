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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.EgressConfig;
import dev.knative.eventing.kafka.broker.core.filter.Filter;
import dev.knative.eventing.kafka.broker.core.filter.impl.AttributesFilter;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.security.Credentials;
import dev.knative.eventing.kafka.broker.core.security.KafkaClientsAuth;
import dev.knative.eventing.kafka.broker.core.security.PlaintextCredentials;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordHandler;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordSender;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.PartitionKeyExtensionInterceptor;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class HttpConsumerVerticleFactory implements ConsumerVerticleFactory {

  private final static ConsumerRecordSender NO_DLQ_SENDER =
    ConsumerRecordSender.create(Future.failedFuture("No DLQ set"), Future.succeededFuture());

  private final Map<String, Object> consumerConfigs;
  private final WebClientOptions webClientOptions;
  private final Map<String, String> producerConfigs;
  private final ConsumerRecordOffsetStrategyFactory consumerRecordOffsetStrategyFactory;
  private final AuthProvider authProvider;

  /**
   * All args constructor.
   *
   * @param consumerRecordOffsetStrategyFactory consumer offset handling strategy
   * @param consumerConfigs                     base consumer configurations.
   * @param webClientOptions                    web client options.
   * @param producerConfigs                     base producer configurations.
   * @param authProvider                        auth provider.
   */
  public HttpConsumerVerticleFactory(
    final ConsumerRecordOffsetStrategyFactory consumerRecordOffsetStrategyFactory,
    final Properties consumerConfigs,
    final WebClientOptions webClientOptions,
    final Properties producerConfigs,
    final AuthProvider authProvider) {

    Objects.requireNonNull(consumerRecordOffsetStrategyFactory, "provide consumerRecordOffsetStrategyFactory");
    Objects.requireNonNull(consumerConfigs, "provide consumerConfigs");
    Objects.requireNonNull(webClientOptions, "provide webClientOptions");
    Objects.requireNonNull(producerConfigs, "provide producerConfigs");

    this.consumerRecordOffsetStrategyFactory = consumerRecordOffsetStrategyFactory;
    this.consumerConfigs = consumerConfigs.entrySet()
      .stream()
      .map(e -> new SimpleImmutableEntry<>(e.getKey().toString(), e.getValue()))
      .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.producerConfigs = producerConfigs.entrySet()
      .stream()
      .map(e -> new SimpleImmutableEntry<>(e.getKey().toString(), e.getValue().toString()))
      .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.webClientOptions = webClientOptions;
    this.authProvider = authProvider;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AbstractVerticle get(final DataPlaneContract.Resource resource, final DataPlaneContract.Egress egress) {

    Objects.requireNonNull(resource, "provide resource");
    Objects.requireNonNull(egress, "provide egress");

    // Consumer and producer configs are shared objects and they act as a prototype for each instance.
    final var consumerConfigs = new HashMap<>(this.consumerConfigs);
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());
    consumerConfigs.put(GROUP_ID_CONFIG, egress.getConsumerGroup());

    final var producerConfigs = new HashMap<>(this.producerConfigs);
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());
    producerConfigs.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PartitionKeyExtensionInterceptor.class.getName());

    final Future<Credentials> credentialsFuture = resource.hasAuthSecret() ?
      authProvider.getCredentials(resource.getAuthSecret().getNamespace(), resource.getAuthSecret().getName())
      : Future.succeededFuture(new PlaintextCredentials());

    final Function<Vertx, Future<KafkaConsumer<String, CloudEvent>>> consumerFactory = createConsumerFactory(
      consumerConfigs,
      resource,
      credentialsFuture
    );

    final Function<Vertx, Future<KafkaProducer<String, CloudEvent>>> producerFactory = createProducerFactory(
      producerConfigs,
      resource,
      credentialsFuture
    );

    final BiFunction<Vertx, KafkaConsumer<String, CloudEvent>, Future<ConsumerRecordHandler>> recordHandlerFactory =
      (vertx, consumer) -> {

        final var egressConfig =
          egress.hasEgressConfig() ?
            egress.getEgressConfig() :
            resource.getEgressConfig();
        final var circuitBreakerOptions = createCircuitBreakerOptions(egressConfig);

        final var egressSubscriberSender = createConsumerRecordSender(
          vertx,
          egress.getDestination(),
          circuitBreakerOptions,
          egressConfig
        );

        final var egressDeadLetterSender = isDeadLetterSinkAbsent(egressConfig)
          ? NO_DLQ_SENDER
          : createConsumerRecordSender(vertx, egressConfig.getDeadLetter(), circuitBreakerOptions, egressConfig);

        return producerFactory.apply(vertx)
          .map(producer -> new ConsumerRecordHandler(
            egressSubscriberSender,
            egress.hasFilter() ? new AttributesFilter(egress.getFilter().getAttributesMap()) : Filter.noop(),
            this.consumerRecordOffsetStrategyFactory.get(consumer, resource, egress),
            new HttpSinkResponseHandler(vertx, resource.getTopics(0), producer),
            egressDeadLetterSender
          ));
      };

    return new ConsumerVerticle(consumerFactory, new HashSet<>(resource.getTopicsList()), recordHandlerFactory);
  }

  protected Function<Vertx, Future<KafkaConsumer<String, CloudEvent>>> createConsumerFactory(
    final Map<String, Object> consumerConfigs,
    final DataPlaneContract.Resource resource,
    final Future<Credentials> credentialsFuture) {
    return vertx -> credentialsFuture
      .compose(credentials -> KafkaClientsAuth.updateConsumerConfigs(credentials, consumerConfigs))
      // Note: Do not use consumerConfigs as parameter, use configs (return value)
      .map(configs -> createConsumer(vertx, configs));
  }

  private static KafkaConsumer<String, CloudEvent> createConsumer(final Vertx vertx,
                                                                  final Map<String, Object> consumerConfigs) {
    final var opt = new KafkaClientOptions()
      .setConfig(consumerConfigs)
      .setTracingPolicy(TracingPolicy.PROPAGATE);

    return KafkaConsumer.create(vertx, opt);
  }

  protected Function<Vertx, Future<KafkaProducer<String, CloudEvent>>> createProducerFactory(
    final Map<String, String> producerConfigs,
    final DataPlaneContract.Resource resource,
    final Future<Credentials> credentialsFuture) {
    return vertx -> credentialsFuture
      .compose(credentials -> KafkaClientsAuth.updateProducerConfigs(credentials, producerConfigs))
      // Note: Do not use producerConfigs as parameter, use configs (return value)
      .map(configs -> KafkaProducer.create(vertx, configs));
  }

  private ConsumerRecordSender createConsumerRecordSender(
    final Vertx vertx,
    final String target,
    final CircuitBreakerOptions circuitBreakerOptions,
    final EgressConfig egress) {

    final var circuitBreaker = CircuitBreaker.create(target, vertx, circuitBreakerOptions);
    circuitBreaker.retryPolicy(computeRetryPolicy(egress));

    return new HttpConsumerRecordSender(
      vertx,
      target,
      circuitBreaker,
      WebClient.create(vertx, this.webClientOptions)
    );
  }

  private static CircuitBreakerOptions createCircuitBreakerOptions(final DataPlaneContract.EgressConfig egressConfig) {
    if (egressConfig.getRetry() > 0) {
      return new CircuitBreakerOptions().setMaxRetries(egressConfig.getRetry());
    }
    return new CircuitBreakerOptions();
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

  private static boolean isDeadLetterSinkAbsent(final EgressConfig egressConfig) {
    return egressConfig == null || egressConfig.getDeadLetter() == null || egressConfig.getDeadLetter().isEmpty();
  }
}
