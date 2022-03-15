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
package dev.knative.eventing.kafka.broker.dispatcher.main;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.EgressConfig;
import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.security.KafkaClientsAuth;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticleFactory;
import dev.knative.eventing.kafka.broker.dispatcher.DeliveryOrder;
import dev.knative.eventing.kafka.broker.dispatcher.Filter;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.NoopResponseHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherImpl;
import dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherMutatorChain;
import dev.knative.eventing.kafka.broker.dispatcher.impl.ResponseToHttpEndpointHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.ResponseToKafkaTopicHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.BaseConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventOverridesMutator;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEventInterceptor;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.KeyDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.OffsetManager;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.OrderedConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.UnorderedConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.AttributesFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.http.WebClientCloudEventSender;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.common.tracing.ConsumerTracer;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class ConsumerVerticleFactoryImpl implements ConsumerVerticleFactory {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerVerticleFactoryImpl.class);

  private final static CloudEventSender NO_DEAD_LETTER_SINK_SENDER = CloudEventSender.noop("No dead letter sink set");

  private final Map<String, Object> consumerConfigs;
  private final WebClientOptions webClientOptions;
  private final Map<String, Object> producerConfigs;
  private final AuthProvider authProvider;
  private final Counter eventsSentCounter;

  /**
   * All args constructor.
   *
   * @param consumerConfigs  base consumer configurations.
   * @param webClientOptions web client options.
   * @param producerConfigs  base producer configurations.
   * @param authProvider     auth provider.
   * @param metricsRegistry  meter registry to use to create metricsRegistry.
   */
  public ConsumerVerticleFactoryImpl(
    final Properties consumerConfigs,
    final WebClientOptions webClientOptions,
    final Properties producerConfigs,
    final AuthProvider authProvider,
    final MeterRegistry metricsRegistry) {

    Objects.requireNonNull(consumerConfigs, "provide consumerConfigs");
    Objects.requireNonNull(webClientOptions, "provide webClientOptions");
    Objects.requireNonNull(producerConfigs, "provide producerConfigs");
    Objects.requireNonNull(metricsRegistry, "provide metricsRegistry");

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
    this.eventsSentCounter = metricsRegistry.counter(Metrics.HTTP_EVENTS_SENT_COUNT);
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
    consumerConfigs.put(KeyDeserializer.KEY_TYPE, egress.getKeyType());
    if (isResourceReferenceDefined(resource.getReference())) {
      // Set the resource reference so that when the interceptor gets a record that is not a CloudEvent, it can set
      // CloudEvents context attributes accordingly (see InvalidCloudEventInterceptor for more information).
      consumerConfigs.put(InvalidCloudEventInterceptor.SOURCE_NAME_CONFIG, resource.getReference().getName());
      consumerConfigs.put(InvalidCloudEventInterceptor.SOURCE_NAMESPACE_CONFIG, resource.getReference().getNamespace());
    }

    final var producerConfigs = new HashMap<>(this.producerConfigs);
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());

    final DeliveryOrder deliveryOrder = DeliveryOrder.fromContract(egress.getDeliveryOrder());

    final BaseConsumerVerticle.Initializer initializer = (vertx, consumerVerticle) ->
      authProvider.getCredentials(resource).onSuccess(credentials -> {
        KafkaClientsAuth.attachCredentials(consumerConfigs, credentials);
        KafkaClientsAuth.attachCredentials(producerConfigs, credentials);

        final KafkaConsumer<Object, CloudEvent> consumer = createConsumer(vertx, consumerConfigs);
        AutoCloseable metricsCloser = Metrics.register(consumer.unwrap());

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
          : NO_DEAD_LETTER_SINK_SENDER;

        final var filter = egress.hasFilter() ?
          new AttributesFilter(egress.getFilter().getAttributesMap()) :
          Filter.noop();

        final var responseHandler = getResponseHandler(egress,
          () -> getResponseToKafkaTopicHandler(vertx, producerConfigs, resource),
          () -> new ResponseToHttpEndpointHandler(createConsumerRecordSender(vertx, egress.getReplyUrl(), egressConfig)));
        final var commitIntervalMs = Integer.parseInt(String.valueOf(consumerConfigs.get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG)));

        final var recordDispatcher = new RecordDispatcherMutatorChain(
          new RecordDispatcherImpl(
            filter,
            egressSubscriberSender,
            egressDeadLetterSender,
            responseHandler,
            new OffsetManager(vertx, consumer, eventsSentCounter::increment, commitIntervalMs),
            ConsumerTracer.create(
              ((VertxInternal) vertx).tracer(),
              new KafkaClientOptions()
                .setConfig(consumerConfigs)
                // Make sure the policy is propagate for the manually instantiated consumer tracer
                .setTracingPolicy(TracingPolicy.PROPAGATE)
            )
          ),
          new CloudEventOverridesMutator(resource.getCloudEventOverrides())
        );

        // Set all the built objects in the consumer verticle
        consumerVerticle.setRecordDispatcher(recordDispatcher);
        consumerVerticle.setConsumer(consumer);
        consumerVerticle.setCloser(AsyncCloseable.wrapAutoCloseable(metricsCloser));
      })
        .mapEmpty();

    return getConsumerVerticle(
      deliveryOrder,
      initializer,
      new HashSet<>(resource.getTopicsList()),
      consumerConfigs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)
    );
  }

  static ResponseHandler getResponseHandler(final DataPlaneContract.Egress egress,
                                            final Supplier<ResponseHandler> kafkaSupplier,
                                            final Supplier<ResponseHandler> httpSupplier) {
    if(egress.hasReplyUrl()){
      return httpSupplier.get();
    } else if(egress.hasReplyToOriginalTopic()){
      return kafkaSupplier.get();
    } else if (egress.hasDiscardReply()) {
      return new NoopResponseHandler();
    }
    //TODO: log
    return kafkaSupplier.get();
  }

  private ResponseToKafkaTopicHandler getResponseToKafkaTopicHandler(final Vertx vertx,
                                                                     final Map<String, Object> producerConfigs,
                                                                     final DataPlaneContract.Resource resource) {
    final KafkaProducer<String, CloudEvent> producer = createProducer(vertx, producerConfigs);
    return new ResponseToKafkaTopicHandler(producer, resource.getTopics(0));
  }

  protected KafkaProducer<String, CloudEvent> createProducer(final Vertx vertx,
                                                             final Map<String, Object> producerConfigs) {
    Properties producerProperties = new Properties();
    producerProperties.putAll(producerConfigs);
    return KafkaProducer.create(vertx, producerProperties);
  }

  protected KafkaConsumer<Object, CloudEvent> createConsumer(final Vertx vertx,
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

  private CloudEventSender createConsumerRecordSender(
    final Vertx vertx,
    final String target,
    final EgressConfig egress) {

    final var circuitBreaker = CircuitBreaker
      .create(target, vertx, createCircuitBreakerOptions(egress))
      .retryPolicy(computeRetryPolicy(egress))
      .openHandler(r -> logger.info("Circuit breaker opened {}", keyValue("target", target)))
      .halfOpenHandler(r -> logger.info("Circuit breaker half-opened {}", keyValue("target", target)))
      .closeHandler(r -> logger.info("Circuit breaker closed {}", keyValue("target", target)));

    return new WebClientCloudEventSender(
      WebClient.create(vertx, this.webClientOptions), circuitBreaker, target
    );
  }

  private static CircuitBreakerOptions createCircuitBreakerOptions(final DataPlaneContract.EgressConfig egressConfig) {
    CircuitBreakerOptions options = new CircuitBreakerOptions();

    // This disables circuit breaker notifications on the event bus
    options.setNotificationAddress(null);

    if (egressConfig != null) {
      // Single request timeout
      options.setTimeout(
        egressConfig.getTimeout() > 0 ?
          egressConfig.getTimeout() :
          CircuitBreakerOptions.DEFAULT_TIMEOUT
      );

      // Retry options
      if (egressConfig.getRetry() > 0) {
        options
          // TODO reset timeout should be configurable or, at least, set by the control plane
          .setResetTimeout(
            egressConfig.getBackoffDelay() > 0 ?
              egressConfig.getBackoffDelay() :
              CircuitBreakerOptions.DEFAULT_RESET_TIMEOUT
          )
          // TODO max failures should be configurable or, at least, set by the control plane
          .setMaxFailures(egressConfig.getRetry() * 2)
          .setMaxRetries(egressConfig.getRetry());
      }
    }
    return options;
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

  private static AbstractVerticle getConsumerVerticle(final DeliveryOrder type,
                                                      final BaseConsumerVerticle.Initializer initializer,
                                                      final Set<String> topics,
                                                      final Object maxPollRecords) {
    return switch (type) {
      case ORDERED -> new OrderedConsumerVerticle(initializer, topics);
      case UNORDERED -> new UnorderedConsumerVerticle(
        initializer, topics, maxPollRecords == null ? 0 : Integer.parseInt(maxPollRecords.toString())
      );
    };
  }

  private static boolean isResourceReferenceDefined(DataPlaneContract.Reference resource) {
    return resource != null && !resource.getNamespace().isBlank() && !resource.getName().isBlank();
  }
}
