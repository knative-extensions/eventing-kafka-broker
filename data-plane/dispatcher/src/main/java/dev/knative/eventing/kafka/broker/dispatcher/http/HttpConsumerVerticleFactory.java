package dev.knative.eventing.kafka.broker.dispatcher.http;

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.Trigger;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordHandler;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

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
   * @param consumerConfigs base consumer configurations.
   * @param client          http client.
   * @param vertx           vertx instance.
   * @param producerConfigs base producer configurations.
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

    final var brokerDLQSender = createSender(broker.deadLetterSink(), circuitBreakerOptions);

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
    final var kafkaProducer = new KafkaProducer<String, CloudEvent>(producerConfigs);
    return io.vertx.kafka.client.producer.KafkaProducer.create(vertx, kafkaProducer);
  }

  protected io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> createConsumer(
      final Vertx vertx,
      final Broker broker,
      final Trigger<CloudEvent> trigger) {

    // TODO check consumer configurations to change per instance
    // consumerConfigs is a shared object and it acts as a prototype for each consumer instance.
    final var consumerConfigs = (Properties) this.consumerConfigs.clone();
    consumerConfigs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, trigger.id());
    // Note: KafkaConsumer instances are not thread-safe.
    // There are methods thread-safe, but in general they're not.
    final var kafkaConsumer = new KafkaConsumer<String, CloudEvent>(consumerConfigs);
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
}
