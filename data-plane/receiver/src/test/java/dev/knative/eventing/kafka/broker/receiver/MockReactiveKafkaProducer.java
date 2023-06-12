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
package dev.knative.eventing.kafka.broker.receiver;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.cloudevents.CloudEvent;
import io.vertx.core.Future;

public class MockReactiveKafkaProducer<K, V> implements ReactiveKafkaProducer<String, CloudEvent> {


    public MockReactiveKafkaProducer() {
    }

    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> flush() {
        return Future.succeededFuture();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, CloudEvent> record) {
        return Future.succeededFuture();
    }

    @Override
    public org.apache.kafka.clients.producer.Producer<String, CloudEvent> unwrap() {
        return new org.apache.kafka.clients.producer.MockProducer<String, CloudEvent>();
    }
    
}
