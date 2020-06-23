package dev.knative.eventing.kafka.broker.dispatcher;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Objects;

/**
 * ConsumerVerticle is responsible for manging the consumer lifecycle.
 *
 * @param <K> record key type.
 * @param <V> record value type.
 */
public final class ConsumerVerticle<K, V> extends AbstractVerticle {

  private final KafkaConsumer<K, V> consumer;
  private final String topic;
  private final Handler<KafkaConsumerRecord<K, V>> recordHandler;

  /**
   * All args constructor.
   *
   * @param consumer      Kafka consumer.
   * @param topic         topic to consume.
   * @param recordHandler handler of consumed Kafka records.
   */
  public ConsumerVerticle(
      final KafkaConsumer<K, V> consumer,
      final String topic,
      final Handler<KafkaConsumerRecord<K, V>> recordHandler) {

    Objects.requireNonNull(consumer, "provide consumer");
    Objects.requireNonNull(topic, "provide topic");
    Objects.requireNonNull(recordHandler, "provide record handler");

    this.recordHandler = recordHandler;
    this.consumer = consumer;
    this.topic = topic;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start(Promise<Void> startPromise) {

    consumer.handler(recordHandler);

    consumer.subscribe(topic, startPromise);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.close(stopPromise);
  }
}
