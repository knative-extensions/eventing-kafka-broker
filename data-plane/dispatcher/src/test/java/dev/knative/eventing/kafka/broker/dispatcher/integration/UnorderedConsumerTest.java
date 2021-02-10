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
package dev.knative.eventing.kafka.broker.dispatcher.integration;

import static dev.knative.eventing.kafka.broker.core.file.FileWatcherTest.write;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.contract;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerDeployerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.OffsetManagerFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(VertxExtension.class)
public class UnorderedConsumerTest {

  static {
    BackendRegistries.setupBackend(new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME));
  }

  private static final Logger logger = LoggerFactory.getLogger(UnorderedConsumerTest.class);
  private static final int NUM_SYSTEM_VERTICLES = 1;

  @Test
  public void testUnorderedConsumer(final Vertx vertx)
    throws IOException, InterruptedException, ExecutionException {
    ContractMessageCodec.register(vertx.eventBus());

    final var producerConfigs = new Properties();
    final var consumerConfigs = new Properties();

    final var consumerVerticleFactoryMock = new ConsumerVerticleFactoryMock(
      consumerConfigs,
      producerConfigs,
      OffsetManagerFactory.unordered(null)
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

    final var consumerDeployer = new ConsumerDeployerVerticle(
      consumerVerticleFactoryMock,
      100
    );

    vertx.deployVerticle(consumerDeployer)
      .toCompletionStage()
      .toCompletableFuture()
      .get();

    final var contract = contract();
    final var numEgresses = contract.getResourcesList().stream()
      .mapToInt(DataPlaneContract.Resource::getEgressesCount)
      .sum();

    final var waitEvents = new CountDownLatch(numEgresses * consumerRecords.size());
    startServer(vertx, new VertxTestContext(), event, waitEvents);

    final var file = Files.createTempFile("fw-", "-fw").toFile();
    final var fileWatcher = new FileWatcher(
      FileSystems.getDefault().newWatchService(),
      new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS),
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

    write(file, contract);

    await().atMost(6, TimeUnit.SECONDS)
      .untilAsserted(() -> assertThat(vertx.deploymentIDs()).hasSize(numEgresses + NUM_SYSTEM_VERTICLES));

    waitEvents.await();

    final var producers = consumerVerticleFactoryMock.producers();
    final var consumers = consumerVerticleFactoryMock.consumers();

    assertThat(producers).hasSameSizeAs(consumers);

    final var events = consumerRecords.stream()
      .map(ConsumerRecord::value)
      .toArray(CloudEvent[]::new);
    final var partitionKeys = Arrays.stream(events)
      .map(e -> {
        final var partitionKey = e.getExtension("partitionkey");
        if (partitionKey == null) {
          return null;
        }
        return partitionKey.toString();
      })
      .collect(Collectors.toList());

    await().atMost(6, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final var producerEntry : producers.entrySet()) {
        final var history = producerEntry.getValue().history();
        assertThat(history).hasSameSizeAs(consumerRecords);

        assertThat(history.stream().map(ProducerRecord::value)).containsExactlyInAnyOrder(events);
        assertThat(history.stream().map(ProducerRecord::key)).containsAnyElementsOf(partitionKeys);
      }
      for (final var consumer : consumers.values()) {
        var key = new TopicPartition("", 0);

        assertThat(consumer.committed(Set.of(key)))
          .extractingByKey(key)
          .extracting(OffsetAndMetadata::offset)
          .isEqualTo(2L);
      }
    });

    fileWatcher.close();
    executorService.shutdown();
  }

  private static void startServer(
    final Vertx vertx,
    final VertxTestContext context,
    final CloudEvent event,
    final CountDownLatch waitEvents) throws InterruptedException {

    final var destinationURL = CoreObjects.DESTINATION_URL;
    vertx.createHttpServer()
      .exceptionHandler(context::failNow)
      // request -> message -> event -> check event -> put event in response
      .requestHandler(request -> VertxMessageFactory
        .createReader(request)
        .onFailure(context::failNow)
        .map(MessageReader::toEvent)
        .onSuccess(receivedEvent -> {
          logger.info("received event {}", event);

          context.verify(() -> {
            assertThat(receivedEvent).isEqualTo(event);

            VertxMessageFactory
              .createWriter(request.response())
              .writeBinary(event);

            waitEvents.countDown();
          });

        })
      )
      .listen(
        destinationURL.getPort(),
        destinationURL.getHost(),
        context.succeedingThenComplete()
      );

    context.awaitCompletion(10, TimeUnit.SECONDS);
  }
}
