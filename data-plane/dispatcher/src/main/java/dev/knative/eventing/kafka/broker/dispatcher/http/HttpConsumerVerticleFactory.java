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
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordSender;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.ConsumerType;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.ConsumerVerticleFactory;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.impl.UnorderedConsumerVerticle;
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
  private final Map<String, Object> producerConfigs;
  private final AuthProvider authProvider;

  /**
   * All args constructor.
   *
   * @param consumerConfigs      base consumer configurations.
   * @param webClientOptions     web client options.
   * @param producerConfigs      base producer configurations.
   * @param authProvider         auth provider.
   */
  public HttpConsumerVerticleFactory(
    final Properties consumerConfigs,
    final WebClientOptions webClientOptions,
    final Properties producerConfigs,
    final AuthProvider authProvider) {

    Objects.requireNonNull(consumerConfigs, "provide consumerConfigs");
    Objects.requireNonNull(webClientOptions, "provide webClientOptions");
    Objects.requireNonNull(producerConfigs, "provide producerConfigs");

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

    // TODO I'm assuming ordered/unordered will be configure here, at egress level
    final ConsumerType type = ConsumerType.UNORDERED;

    final ABC initializer = vertx -> {
      (resource.hasAuthSecret() ?
        authProvider.getCredentials(resource.getAuthSecret().getNamespace(), resource.getAuthSecret().getName()) :
        Future.succeededFuture(new PlaintextCredentials())
      ).compose(credentials -> {
        KafkaClientsAuth.attachCredentials(consumerConfigs, credentials);
        KafkaClientsAuth.attachCredentials(producerConfigs, credentials);

        KafkaConsumer<String, CloudEvent> consumer = KafkaConsumer.create(
          vertx,
          new KafkaClientOptions()
            .setConfig(consumerConfigs)
            .setTracingPolicy(TracingPolicy.PROPAGATE)
        );

        KafkaProducer<String, CloudEvent> producer = KafkaProducer.create(
          vertx,
          new KafkaClientOptions().setConfig(producerConfigs)
        );

        final var circuitBreakerOptions = createCircuitBreakerOptions(resource);
        final var egressConfig = resource.getEgressConfig();

        final var egressSubscriberSender = createConsumerRecordSender(
          vertx,
          egress.getDestination(),
          circuitBreakerOptions,
          egressConfig
        );

        final var egressDeadLetterSender = hasDeadLetterSink(egressConfig)
          ? createConsumerRecordSender(vertx, egressConfig.getDeadLetter(), circuitBreakerOptions, egressConfig)
          : NO_DLQ_SENDER;

        final RecordDispatcher recordDispatcher = new RecordDispatcher(
          egress.hasFilter() ? new AttributesFilter(egress.getFilter().getAttributesMap()) : Filter.noop(),
          egressSubscriberSender,
          egressDeadLetterSender,
          new HttpSinkResponseHandler(vertx, resource.getTopics(0), producer),
          type.getOffsetManager(consumer, null) //TODO
        );

      });
    };

    final Future<Credentials> credentialsFuture = resource.hasAuthSecret() ?
      authProvider.getCredentials(resource.getAuthSecret().getNamespace(), resource.getAuthSecret().getName()) :
      Future.succeededFuture(new PlaintextCredentials());


    final BiFunction<Vertx, KafkaConsumer<String, CloudEvent>, Future<RecordDispatcher>> recordDispatcherFactory =
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

        final var egressDeadLetterSender = hasDeadLetterSink(egressConfig)
          ? createConsumerRecordSender(vertx, egressConfig.getDeadLetter(), circuitBreakerOptions, egressConfig)
          : NO_DLQ_SENDER;

        return producerFactory.apply(vertx)
          .map(producer -> new RecordDispatcher(
            egress.hasFilter() ? new AttributesFilter(egress.getFilter().getAttributesMap()) : Filter.noop(),
            egressSubscriberSender,
            egressDeadLetterSender,
            new HttpSinkResponseHandler(vertx, resource.getTopics(0), producer),
            type.getOffsetManager(consumer, null) //TODO
          ));
      };

    return new UnorderedConsumerVerticle(consumer, recordDispatcher, new HashSet<>(resource.getTopicsList()));
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
        case Linear -> retryCount -> delay * retryCount;
        case Exponential, UNRECOGNIZED -> retryCount -> delay * Math.round(Math.pow(2, retryCount));
      };
    }
    return retry -> 0L; // Default Vert.x retry policy, it means don't retry
  }

  private static boolean hasDeadLetterSink(final EgressConfig egressConfig) {
    return !(egressConfig == null || egressConfig.getDeadLetter() == null || egressConfig.getDeadLetter().isEmpty());
  }
}
