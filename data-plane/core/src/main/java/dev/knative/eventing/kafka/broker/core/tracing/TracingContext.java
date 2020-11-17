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

import io.opentelemetry.api.trace.propagation.HttpTraceContext;
import io.opentelemetry.context.Context;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

// TODO figure out what current means in our world.
final public class TracingContext {

  public static Context from(final Iterable<Entry<String, String>> headers) {
    return HttpTraceContext
      .getInstance()
      .extract(Context.current(), headers, new HttpHeadersPropagatorGetter());
  }

  public static <K, V> Context from(final KafkaConsumerRecord<K, V> record) {
    return HttpTraceContext
      .getInstance()
      .extract(Context.current(), record, new KafkaConsumerRecordGetter<>());
  }

  public static <K, V> void to(final Context context, final KafkaProducerRecord<K, V> record) {
    HttpTraceContext
      .getInstance()
      .inject(context, record, new KafkaProducerRecordSetter<>());
  }

  public static void to(final Context context, final BiConsumer<String, String> headers) {
    HttpTraceContext
      .getInstance()
      .inject(context, headers, new HttpHeadersPropagatorSetter());
  }
}
