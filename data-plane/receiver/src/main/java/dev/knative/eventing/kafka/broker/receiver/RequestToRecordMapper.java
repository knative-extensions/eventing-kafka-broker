package dev.knative.eventing.kafka.broker.receiver;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

@FunctionalInterface
public interface RequestToRecordMapper<K, V> {

  /**
   * Map the given HTTP request to a Kafka record.
   *
   * @param request http request.
   * @return kafka record (record can be null).
   */
  Future<KafkaProducerRecord<K, V>> recordFromRequest(final HttpServerRequest request);
}
