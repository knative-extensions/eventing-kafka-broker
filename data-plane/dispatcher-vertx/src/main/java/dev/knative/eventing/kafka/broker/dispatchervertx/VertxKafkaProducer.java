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
package dev.knative.eventing.kafka.broker.dispatchervertx;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import dev.knative.eventing.kafka.broker.dispatcher.ReactiveKafkaProducer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class VertxKafkaProducer<K, V> implements ReactiveKafkaProducer<K, V>{

    private final KafkaProducer<K, V> producer;

    VertxKafkaProducer(Vertx v, Map<String, Object> configs) {
        Properties producerProperties = new Properties();
        producerProperties.putAll(configs);
        this.producer = KafkaProducer.create(v, producerProperties);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return this.producer.send(KafkaProducerRecord.create(record.topic(), record.value())).map(
            vertxRecordMetadata -> new RecordMetadata(
                    new TopicPartition(record.topic(), vertxRecordMetadata.getPartition()),
                        vertxRecordMetadata.getOffset(),0,vertxRecordMetadata.getTimestamp(),-1,-1));
    }

    @Override
    public Future<Void> close() {
        return producer.close();
    }

    @Override
    public Producer<K,V> unwrap() {
        return producer.unwrap();
    }
    
}
