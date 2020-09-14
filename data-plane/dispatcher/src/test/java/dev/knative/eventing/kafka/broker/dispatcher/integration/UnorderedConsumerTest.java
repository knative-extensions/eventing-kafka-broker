/*
 * Copyright 2020 The Knative Authors
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
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.contract;
import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.ObjectsCreator;
import dev.knative.eventing.kafka.broker.core.cloudevents.PartitionKey;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.ResourcesManager;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
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
import java.util.stream.Collectors;
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
    final var client = WebClient.create(vertx);

    final var consumerVerticleFactoryMock = new ConsumerVerticleFactoryMock(
      consumerConfigs,
      client,
      vertx,
      producerConfigs,
      ConsumerRecordOffsetStrategyFactory.unordered()
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

    final var resourcesManager = new ResourcesManager(
      vertx,
      consumerVerticleFactoryMock,
      100,
      100
    );

    final var objectsCreator = new ObjectsCreator(resourcesManager);

    final var contract = contract();
    final var numEgresses = contract.getResourcesList().stream()
      .mapToInt(DataPlaneContract.Resource::getEgressesCount)
      .sum();

    final var waitEvents = new CountDownLatch(numEgresses * consumerRecords.size());
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

    write(file, contract);

    Thread.sleep(6000); // reduce flakiness

    assertThat(vertx.deploymentIDs())
      .hasSize(numEgresses);

    waitEvents.await();

    final var producers = consumerVerticleFactoryMock.producers();
    final var consumers = consumerVerticleFactoryMock.consumers();

    assertThat(producers).hasSameSizeAs(consumers);

    final var events = consumerRecords.stream()
      .map(ConsumerRecord::value)
      .toArray(CloudEvent[]::new);
    final var partitionKeys = Arrays.stream(events)
      .map(PartitionKey::extract)
      .collect(Collectors.toList());

    for (final var producerEntry : producers.entrySet()) {
      final var history = producerEntry.getValue().history();
      assertThat(history).hasSameSizeAs(consumerRecords);

      assertThat(history.stream().map(ProducerRecord::value))
        .containsExactlyInAnyOrder(events);
      assertThat(history.stream().map(ProducerRecord::key))
        .containsAnyElementsOf(partitionKeys);
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
      .requestHandler(request -> VertxMessageFactory
        .createReader(request)
        .onFailure(context::failNow)
        .map(MessageReader::toEvent)
        .onSuccess(receivedEvent -> {
          logger.info("received event {}", event);

          context.verify(() -> {
            assertThat(receivedEvent).isEqualTo(event);
            waitEvents.countDown();
          });

          VertxMessageFactory
            .createWriter(request.response())
            .writeBinary(event);
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
