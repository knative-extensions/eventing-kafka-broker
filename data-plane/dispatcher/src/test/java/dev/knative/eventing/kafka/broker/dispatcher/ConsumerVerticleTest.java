package dev.knative.eventing.kafka.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ConsumerVerticleTest {

  @Test
  public void subscribedToTopic(final Vertx vertx, final VertxTestContext context) {
    final var consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final var topic = "topic1";

    final var kafkaConsumer = KafkaConsumer.create(vertx, consumer);
    final var verticle = new ConsumerVerticle<>(kafkaConsumer, topic, record -> fail());

    final Promise<Void> promise = Promise.promise();
    verticle.start(promise);

    promise.future()
        .onSuccess(ignored -> {
          assertThat(consumer.subscription()).containsExactlyInAnyOrder(topic);
          assertThat(consumer.closed()).isFalse();
          context.completeNow();
        })
        .onFailure(Assertions::fail);
  }

  @Test
  public void stop(final Vertx vertx, final VertxTestContext context) {
    final var consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final var topic = "topic1";

    final var kafkaConsumer = KafkaConsumer.create(vertx, consumer);
    final var verticle = new ConsumerVerticle<>(kafkaConsumer, topic, record -> fail());

    final Promise<Void> promise = Promise.promise();
    verticle.start((Promise<Void>) null);
    verticle.stop(promise);

    promise.future()
        .onSuccess(ignored -> {
          assertThat(consumer.closed()).isTrue();
          context.completeNow();
        })
        .onFailure(Assertions::fail);

  }
}