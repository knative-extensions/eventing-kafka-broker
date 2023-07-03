package dev.knative.eventing.kafka.broker.dispatcher;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.vertx.core.Future;

public interface ReactiveKafkaProducer<K, V> {

        /**
     * Send a record to a Kafka topic
     *
     * @param record the record to send
     * @return a future completed with the record metadata or with a failure if the
     *         record cannot be sent
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * Close the producer
     *
     * @return a future notifying when the producer is closed
     */
    Future<Void> close();

    // /**
    //  * Flush the producer
    //  * <p>
    //  * This method ensures all records stored in the producer buffer are actually
    //  * sent to the Kafka cluster.
    //  *
    //  * @return a future notifying when the producer is flushed
    //  */
    // Future<Void> flush();

    /**
     * Wnrap the underlying Kafka producer
     * <p>
     * Use this if you want to access the Kafka producer directly.
     *
     * @return the instance underlying KafkaProducer
     */
    Producer<K, V> unwrap();
    
}
