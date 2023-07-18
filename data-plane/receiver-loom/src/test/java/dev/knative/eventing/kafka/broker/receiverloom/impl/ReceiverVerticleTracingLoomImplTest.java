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
package dev.knative.eventing.kafka.broker.receiverloom.impl;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.receiver.impl.ReceiverVerticleTracingTest;
import dev.knative.eventing.kafka.broker.receiverloom.LoomKafkaProducer;
import io.cloudevents.CloudEvent;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.producer.Producer;

public class ReceiverVerticleTracingLoomImplTest extends ReceiverVerticleTracingTest {

    @Override
    public ReactiveKafkaProducer<String, CloudEvent> createKafkaProducer(
            Vertx vertx, Producer<String, CloudEvent> producer) {
        return new LoomKafkaProducer<>(vertx, producer);
    }
}
