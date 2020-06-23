package dev.knative.eventing.kafka.broker.core;

import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker1;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker2;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.brokers;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger1;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger2;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger3;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger4;
import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.config.BrokersConfig;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Brokers;
import io.vertx.core.Future;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
public class ObjectsCreatorTest {

  @Test
  public void shouldNotPassBrokerWithNoTrigger() {
    final var called = new AtomicBoolean(false);

    final var brokers = Map.of();

    final var creator = new ObjectsCreator(objects -> {
      called.set(true);
      assertThat(objects).usingRecursiveComparison().isEqualTo(brokers);
      return Future.succeededFuture();
    });

    creator.accept(Brokers.newBuilder()
        .addBroker(BrokersConfig.Broker.newBuilder()
            .setTopic("1234")
            .setDeadLetterSink("http://localhost:9090")
            .build())
        .build()
    );

    assertThat(called.get()).isTrue();
  }

  @Test
  public void shouldPassAllTriggers() {
    final var called = new AtomicBoolean(false);

    final var brokers = Map.of(
        broker1(), Set.of(trigger1(), trigger2()),
        broker2(), Set.of(trigger3(), trigger4())
    );

    final var creator = new ObjectsCreator(objects -> {
      called.set(true);
      assertThat(objects).usingRecursiveComparison().isEqualTo(brokers);
      return Future.succeededFuture();
    });

    creator.accept(brokers());

    assertThat(called.get()).isTrue();
  }
}