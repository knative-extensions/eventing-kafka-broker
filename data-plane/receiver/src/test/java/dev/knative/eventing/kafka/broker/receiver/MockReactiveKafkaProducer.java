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

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MockReactiveKafkaProducer<K, V> implements ReactiveKafkaProducer<K, V> {

    private final Producer<K, V> producer;

    public MockReactiveKafkaProducer(Producer<K, V> producer) {
        this.producer = producer;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        final Promise<RecordMetadata> p = Promise.promise();
        producer.send(record, (recordMetadata, exception) -> {
            if (exception != null) {
                p.fail(exception);
            } else {
                p.complete(recordMetadata);
            }
        });
        return p.future();
    }

    @Override
    public Future<Void> close() {
        producer.close();
        return Future.succeededFuture();
    }

    @Override
    public Producer<K, V> unwrap() {
        return producer;
    }

    @Override
    public Future<Void> flush() {
        producer.flush();
        return Future.succeededFuture();
    }
}
