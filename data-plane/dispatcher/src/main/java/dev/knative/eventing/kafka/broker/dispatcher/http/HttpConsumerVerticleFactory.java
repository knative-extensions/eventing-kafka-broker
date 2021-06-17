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
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.security.KafkaClientsAuth;
import dev.knative.eventing.kafka.broker.core.security.PlaintextCredentials;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordSender;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.ConsumerVerticleFactory;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.DeliveryOrder;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.OffsetManager;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.impl.BaseConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.impl.OrderedConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.impl.OrderedOffsetManager;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.impl.UnorderedConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.impl.UnorderedOffsetManager;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.Counter;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.common.tracing.ConsumerTracer;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class HttpConsumerVerticleFactory implements ConsumerVerticleFactory {

  private static final Logger logger = LoggerFactory.getLogger(HttpConsumerVerticleFactory.class);

  private final static ConsumerRecordSender NO_DLS_SENDER =
    ConsumerRecordSender.create(Future.failedFuture("No dead letter sink set"), Future.succeededFuture());

  private final Map<String, Object> consumerConfigs;
  private final WebClientOptions webClientOptions;
  private final Map<String, Object> producerConfigs;
  private final AuthProvider authProvider;
  private final Counter eventsSentCounter;

  /**
   * All args constructor.
   *
   * @param consumerConfigs   base consumer configurations.
   * @param webClientOptions  web client options.
   * @param producerConfigs   base producer configurations.
   * @param authProvider      auth provider.
   * @param eventsSentCounter events sent counter.
   */
  public HttpConsumerVerticleFactory(
    final Properties consumerConfigs,
    final WebClientOptions webClientOptions,
    final Properties producerConfigs,
    final AuthProvider authProvider,
    final Counter eventsSentCounter) {

    Objects.requireNonNull(consumerConfigs, "provide consumerConfigs");
    Objects.requireNonNull(webClientOptions, "provide webClientOptions");
    Objects.requireNonNull(producerConfigs, "provide producerConfigs");
    Objects.requireNonNull(eventsSentCounter, "provide eventsSentCounter");

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
    this.eventsSentCounter = eventsSentCounter;
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
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, egress.getConsumerGroup());

    final var producerConfigs = new HashMap<>(this.producerConfigs);
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());

    final DeliveryOrder deliveryOrder = DeliveryOrder.fromContract(egress.getDeliveryOrder());

    final BaseConsumerVerticle.Initializer initializer = (vertx, consumerVerticle) ->
      (resource.hasAuthSecret() ?
        authProvider.getCredentials(resource.getAuthSecret().getNamespace(), resource.getAuthSecret().getName()) :
        Future.succeededFuture(new PlaintextCredentials())
      ).onSuccess(credentials -> {
        KafkaClientsAuth.attachCredentials(consumerConfigs, credentials);
        KafkaClientsAuth.attachCredentials(producerConfigs, credentials);

        KafkaConsumer<String, CloudEvent> consumer = createConsumer(vertx, consumerConfigs);
        AutoCloseable metricsCloser = Metrics.register(consumer.unwrap());

        KafkaProducer<String, CloudEvent> producer = createProducer(vertx, producerConfigs);

        final var egressConfig =
          egress.hasEgressConfig() ?
            egress.getEgressConfig() :
            resource.getEgressConfig();

        final var egressSubscriberSender = createConsumerRecordSender(
          vertx,
          egress.getDestination(),
          egressConfig
        );

        final var egressDeadLetterSender = hasDeadLetterSink(egressConfig)
          ? createConsumerRecordSender(vertx, egressConfig.getDeadLetter(), egressConfig)
          : NO_DLS_SENDER;

        final var filter = egress.hasFilter() ?
          new AttributesFilter(egress.getFilter().getAttributesMap()) :
          Filter.noop();

        final RecordDispatcher recordDispatcher = new RecordDispatcher(
          filter,
          egressSubscriberSender,
          egressDeadLetterSender,
          new HttpSinkResponseHandler(vertx, resource.getTopics(0), producer),
          getOffsetManager(deliveryOrder, consumer, eventsSentCounter::increment),
          ConsumerTracer.create(
            ((VertxInternal) vertx).tracer(),
            new KafkaClientOptions()
              .setConfig(consumerConfigs)
              // Make sure the policy is propagate for the manually instantiated consumer tracer
              .setTracingPolicy(TracingPolicy.PROPAGATE)
          )
        );

        // Set all the built objects in the consumer verticle
        consumerVerticle.setRecordDispatcher(recordDispatcher);
        consumerVerticle.setConsumer(consumer);
        consumerVerticle.setCloser(() -> Metrics.close(vertx, metricsCloser));
      })
        .mapEmpty();

    return getConsumerVerticle(deliveryOrder, initializer, new HashSet<>(resource.getTopicsList()));
  }

  protected KafkaProducer<String, CloudEvent> createProducer(final Vertx vertx,
                                                             final Map<String, Object> producerConfigs) {
    Properties producerProperties = new Properties();
    producerProperties.putAll(producerConfigs);
    return KafkaProducer.create(vertx, producerProperties);
  }

  protected KafkaConsumer<String, CloudEvent> createConsumer(final Vertx vertx,
                                                             final Map<String, Object> consumerConfigs) {
    return KafkaConsumer.create(
      vertx,
      new KafkaClientOptions()
        .setConfig(consumerConfigs)
        // Disable tracing provided by vertx-kafka-client, because it doesn't work well with our dispatch logic.
        // RecordDispatcher, when receiving a new record, takes care of adding the proper receive record span.
        .setTracingPolicy(TracingPolicy.IGNORE)
    );
  }

  private ConsumerRecordSender createConsumerRecordSender(
    final Vertx vertx,
    final String target,
    final EgressConfig egress) {

    final var circuitBreaker = CircuitBreaker
      .create(target, vertx, createCircuitBreakerOptions(egress))
      .retryPolicy(computeRetryPolicy(egress))
      .openHandler(r -> logger.info("Circuit breaker opened {}", keyValue("target", target)))
      .halfOpenHandler(r -> logger.info("Circuit breaker half-opened {}", keyValue("target", target)))
      .closeHandler(r -> logger.info("Circuit breaker closed {}", keyValue("target", target)));

    return new HttpConsumerRecordSender(
      vertx,
      target,
      circuitBreaker,
      WebClient.create(vertx, this.webClientOptions)
    );
  }

  private static CircuitBreakerOptions createCircuitBreakerOptions(final DataPlaneContract.EgressConfig egressConfig) {
    if (egressConfig != null && egressConfig.getRetry() > 0) {
      return new CircuitBreakerOptions()
        // TODO reset timeout should be configurable or, at least, set by the control plane
        .setResetTimeout(
          egressConfig.getBackoffDelay() > 0 ?
            egressConfig.getBackoffDelay() :
            CircuitBreakerOptions.DEFAULT_RESET_TIMEOUT
        )
        // TODO max failures should be configurable or, at least, set by the control plane
        .setMaxFailures(egressConfig.getRetry() * 2)
        .setMaxRetries(egressConfig.getRetry())
        // This disables circuit breaker notifications on the event bus
        .setNotificationAddress(null);
    }
    return new CircuitBreakerOptions()
      // This disables circuit breaker notifications on the event bus
      .setNotificationAddress(null);
  }

  /* package visibility for test */
  static Function<Integer, Long> computeRetryPolicy(final EgressConfig egress) {
    if (egress != null && egress.getBackoffDelay() > 0) {
      final var delay = egress.getBackoffDelay();
      return switch (egress.getBackoffPolicy()) {
        case Linear -> retryCount -> delay * retryCount;
        case Exponential, UNRECOGNIZED -> retryCount -> delay * Math.round(Math.pow(2, retryCount));
      };
    }
    return retry -> 0L; // Default Vert.x retry policy, it means don't retry
  }

  private static boolean hasDeadLetterSink(final EgressConfig egressConfig) {
    return !(egressConfig == null || egressConfig.getDeadLetter().isEmpty());
  }

  private static OffsetManager getOffsetManager(final DeliveryOrder type, final KafkaConsumer<?, ?> consumer,
                                                Consumer<Integer> commitHandler) {
    return switch (type) {
      case ORDERED -> new OrderedOffsetManager(consumer, commitHandler);
      case UNORDERED -> new UnorderedOffsetManager(consumer, commitHandler);
    };
  }

  private static AbstractVerticle getConsumerVerticle(final DeliveryOrder type,
                                                      final BaseConsumerVerticle.Initializer initializer,
                                                      final Set<String> topics) {
    return switch (type) {
      case ORDERED -> new OrderedConsumerVerticle(initializer, topics);
      case UNORDERED -> new UnorderedConsumerVerticle(initializer, topics);
    };
  }
}
