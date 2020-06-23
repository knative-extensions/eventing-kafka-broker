package dev.knative.eventing.kafka.broker.dispatcher.integration;

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.Trigger;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.http.HttpConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
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
      final HttpClient client,
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
      final Broker broker,
      final Trigger<CloudEvent> trigger) {

    final var producer = new MockProducer<>(
        true,
        new StringSerializer(),
        new CloudEventSerializer()
    );

    mockProducer.put(trigger.id(), producer);

    return KafkaProducer.create(vertx, producer);
  }

  @Override
  protected KafkaConsumer<String, CloudEvent> createConsumer(
      final Vertx vertx,
      final Broker broker,
      final Trigger<CloudEvent> trigger) {

    final var consumer = new MockConsumer<String, CloudEvent>(OffsetResetStrategy.LATEST);

    mockConsumer.put(trigger.id(), consumer);

    consumer.schedulePollTask(() -> {
      consumer.unsubscribe();

      consumer.assign(records.stream()
          .map(r -> new TopicPartition(broker.topic(), r.partition()))
          .collect(Collectors.toList()));

      for (final var record : records) {
        consumer.addRecord(new ConsumerRecord<>(
            broker.topic(),
            record.partition(),
            record.offset(),
            record.key(),
            record.value()
        ));
        consumer.addEndOffsets(Map.of(
            new TopicPartition(broker.topic(), record.partition()), 0L
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
