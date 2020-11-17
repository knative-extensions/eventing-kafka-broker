/*
 * Copyright 2020 The Knative Authors
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

package dev.knative.eventing.kafka.broker.core.tracing;

import io.opentelemetry.context.propagation.TextMapPropagator.Setter;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

final class KafkaProducerRecordSetter<K, V> implements Setter<KafkaProducerRecord<K, V>> {

  @Override
  public void set(final KafkaProducerRecord<K, V> record, final String key, final String value) {
    if (record == null) {
      return;
    }

    record.addHeader(key, value);
  }
}
