package dev.knative.eventing.kafka.broker.dispatcher.integration;

import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.brokers;
import static dev.knative.eventing.kafka.broker.dispatcher.file.FileWatcherTest.write;
import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.ObjectsCreator;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Broker;
import dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects;
import dev.knative.eventing.kafka.broker.dispatcher.BrokersManager;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.file.FileWatcher;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(VertxExtension.class)
public class UnorderedConsumerTest {

  private static final Logger logger = LoggerFactory.getLogger(UnorderedConsumerTest.class);

  @Test
  public void testUnorderedConsumer(final Vertx vertx, final VertxTestContext context)
      throws IOException, InterruptedException {

    final var producerConfigs = new Properties();
    final var consumerConfigs = new Properties();
    final var client = vertx.createHttpClient();

    final var consumerVerticleFactoryMock = new ConsumerVerticleFactoryMock(
        consumerConfigs,
        client,
        vertx,
        producerConfigs,
        ConsumerRecordOffsetStrategyFactory.create()
    );

    final var event = new CloudEventBuilder()
        .withType("dev.knative")
        .withSubject("subject-1")
        .withSource(URI.create("/api"))
        .withId("1234")
        .build();

    final var consumerRecords = Arrays.asList(
        new ConsumerRecord<>("", 0, 0, "", event),
        new ConsumerRecord<>("", 0, 1, "", event),
        new ConsumerRecord<>("", 0, 2, "", event)
    );
    consumerVerticleFactoryMock.setRecords(consumerRecords);

    final var brokersManager = new BrokersManager<>(
        vertx,
        consumerVerticleFactoryMock,
        100,
        100
    );

    final var objectsCreator = new ObjectsCreator(brokersManager);

    final var brokers = brokers();
    final var numTriggers = brokers.getBrokerList().stream()
        .mapToInt(Broker::getTriggersCount)
        .sum();

    final var waitEvents = new CountDownLatch(numTriggers * consumerRecords.size());
    startServer(vertx, context, event, waitEvents);

    final var file = Files.createTempFile("fw-", "-fw").toFile();
    final var fileWatcher = new FileWatcher(
        FileSystems.getDefault().newWatchService(),
        objectsCreator,
        file
    );

    final var executorService = Executors.newFixedThreadPool(1);
    executorService.submit(() -> {
      try {
        fileWatcher.watch();
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
    });

    write(file, brokers);

    Thread.sleep(6000); // reduce flakiness

    assertThat(vertx.deploymentIDs()).hasSize(numTriggers);

    waitEvents.await();

    final var producers = consumerVerticleFactoryMock.producers();
    final var consumers = consumerVerticleFactoryMock.consumers();

    assertThat(producers).hasSameSizeAs(consumers);

    final var events = consumerRecords.stream()
        .map(ConsumerRecord::value)
        .toArray(CloudEvent[]::new);

    for (final var producerEntry : producers.entrySet()) {
      final var history = producerEntry.getValue().history();
      assertThat(history).hasSameSizeAs(consumerRecords);
      assertThat(history.stream().map(ProducerRecord::value)).containsExactlyInAnyOrder(events);
      // TODO add key check
      assertThat(history.stream().map(ProducerRecord::key)).containsAnyOf("");
    }

    executorService.shutdown();
    context.completeNow();
  }

  private static void startServer(
      final Vertx vertx,
      final VertxTestContext context,
      final CloudEvent event,
      final CountDownLatch waitEvents) throws InterruptedException {

    final var serverStarted = new CountDownLatch(1);
    final var destinationURL = CoreObjects.DESTINATION_URL;
    vertx.createHttpServer()
        .exceptionHandler(context::failNow)
        // request -> message -> event -> check event -> put event in response
        .requestHandler(request -> VertxMessageFactory.createReader(request)
            .onFailure(context::failNow)
            .map(MessageReader::toEvent)
            .onSuccess(receivedEvent -> {

              logger.info("received event {}", event);

              context.verify(() -> {
                assertThat(receivedEvent).isEqualTo(event);
                waitEvents.countDown();
              });

              VertxMessageFactory.createWriter(request.response()).writeBinary(event);
            })
        )
        .listen(
            destinationURL.getPort(),
            destinationURL.getHost(),
            context.succeeding(h -> serverStarted.countDown())
        );

    serverStarted.await();
  }
}
