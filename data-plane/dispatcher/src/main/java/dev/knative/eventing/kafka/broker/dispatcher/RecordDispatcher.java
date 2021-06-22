package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

/**
 * This class performs the dispatch of consumed records.
 */
public interface RecordDispatcher extends AsyncCloseable {

  Future<Void> dispatch(KafkaConsumerRecord<String, CloudEvent> record);

}
