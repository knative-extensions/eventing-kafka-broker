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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.http.HttpConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

public class ConsumerVerticleFactoryMock extends HttpConsumerVerticleFactory {

  // trigger.id() -> Mock*er
  private final Map<String, MockProducer<String, CloudEvent>> mockProducer;
  private final Map<String, MockConsumer<String, CloudEvent>> mockConsumer;

  private List<ConsumerRecord<String, CloudEvent>> records;

  public ConsumerVerticleFactoryMock(
    final Properties consumerConfigs,
    final WebClient client,
    final Vertx vertx,
    final Properties producerConfigs,
    final ConsumerRecordOffsetStrategyFactory<String, CloudEvent> consumerRecordOffsetStrategyFactory) {

    super(consumerRecordOffsetStrategyFactory, consumerConfigs, client, vertx, producerConfigs);
    mockProducer = new ConcurrentHashMap<>();
    mockConsumer = new ConcurrentHashMap<>();
  }

  @Override
  protected KafkaProducer<String, CloudEvent> createProducer(
    final Vertx vertx,
    final DataPlaneContract.Resource resource,
    final DataPlaneContract.Egress egress) {

    final var producer = new MockProducer<>(
      true,
      new StringSerializer(),
      new CloudEventSerializer()
    );

    mockProducer.put(egress.getConsumerGroup(), producer);

    return KafkaProducer.create(vertx, producer);
  }

  @Override
  protected KafkaConsumer<String, CloudEvent> createConsumer(
    final Vertx vertx,
    final DataPlaneContract.Resource resource,
    final DataPlaneContract.Egress egress) {

    final var consumer = new MockConsumer<String, CloudEvent>(OffsetResetStrategy.LATEST);

    mockConsumer.put(egress.getConsumerGroup(), consumer);

    consumer.schedulePollTask(() -> {
      consumer.unsubscribe();

      consumer.assign(records.stream()
        .map(r -> new TopicPartition(resource.getTopics(0), r.partition()))
        .collect(Collectors.toList()));

      for (final var record : records) {
        consumer.addRecord(new ConsumerRecord<>(
          resource.getTopics(0),
          record.partition(),
          record.offset(),
          record.key(),
          record.value()
        ));
        consumer.updateEndOffsets(Map.of(
          new TopicPartition(resource.getTopics(0), record.partition()), 0L
        ));
      }
    });

    return KafkaConsumer.create(vertx, consumer);
  }

  public void setRecords(final List<ConsumerRecord<String, CloudEvent>> records) {
    this.records = records;
  }

  Map<String, MockProducer<String, CloudEvent>> producers() {
    return mockProducer;
  }

  Map<String, MockConsumer<String, CloudEvent>> consumers() {
    return mockConsumer;
  }
}
