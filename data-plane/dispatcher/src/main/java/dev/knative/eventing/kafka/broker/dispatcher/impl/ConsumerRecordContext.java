package dev.knative.eventing.kafka.broker.dispatcher.impl;

import io.cloudevents.CloudEvent;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

class ConsumerRecordContext {

  private KafkaConsumerRecord<Object, CloudEvent> record;
  private long receivedAtMs;

  ConsumerRecordContext(KafkaConsumerRecord<Object, CloudEvent> record) {
    this.record = record;
    this.resetTimer();
  }

  void resetTimer() {
    this.receivedAtMs = System.currentTimeMillis();
  }

  long performLatency() {
    return System.currentTimeMillis() - receivedAtMs;
  }

  KafkaConsumerRecord<Object, CloudEvent> getRecord() {
    return record;
  }

  void setRecord(final KafkaConsumerRecord<Object, CloudEvent> record) {
    this.record = record;
  }
}
