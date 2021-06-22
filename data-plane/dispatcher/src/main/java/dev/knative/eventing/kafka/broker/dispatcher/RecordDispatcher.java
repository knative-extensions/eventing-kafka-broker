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
