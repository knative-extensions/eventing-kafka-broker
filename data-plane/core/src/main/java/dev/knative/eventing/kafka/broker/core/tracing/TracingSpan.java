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

import io.cloudevents.CloudEvent;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Span.Kind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.attributes.SemanticAttributes;
import io.opentelemetry.context.Context;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.function.Consumer;
import org.apache.kafka.common.record.TimestampType;

final public class TracingSpan {

  public final static String SERVICE_NAME;
  public final static String SERVICE_NAMESPACE;

  static {
    SERVICE_NAME = fromEnvOrDefault("SERVICE_NAME", "knative");
    SERVICE_NAMESPACE = fromEnvOrDefault("SERVICE_NAMESPACE", "knative");
  }

  public static class Logging {

    public static final String TRACE_ID = "traceID";
    public static final String SPAN_ID = "spanID";

  }

  public static Span from(final Context context, final Tracer tracer, final HttpServerRequest request) {
    return tracer
      // Span name should be based on the ingress strategy (host or path).
      // We use only path-based routing for now.
      .spanBuilder("receiver")
      .setSpanKind(Kind.SERVER)
      .setAttribute(SemanticAttributes.SERVICE_NAME, SERVICE_NAME)
      .setAttribute(SemanticAttributes.SERVICE_NAMESPACE, SERVICE_NAMESPACE)
      .setAttribute(SemanticAttributes.HTTP_METHOD, request.method().name())
      .setAttribute(SemanticAttributes.HTTP_HOST, request.host())
      .setAttribute(SemanticAttributes.HTTP_SCHEME, request.scheme())
      .setAttribute(SemanticAttributes.HTTP_ROUTE, request.path())
      .setParent(context)
      .startSpan();
  }

  public static <K, V> Span newProducerChild(
    final Context context,
    final Tracer tracer,
    final Span parentSpan,
    final KafkaProducerRecord<K, V> record,
    final Consumer<Span> onStartNewSpan) {

    final var newSpan = tracer
      .spanBuilder(record.topic())
      .setSpanKind(Kind.PRODUCER)
      .setAttribute(SemanticAttributes.MESSAGING_DESTINATION, record.topic())
      .setAttribute(SemanticAttributes.MESSAGING_KAFKA_MESSAGE_KEY, messageKeyAttributeValue(record.key()))
      .setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "kafka")
      .setAttribute(SemanticAttributes.MESSAGING_OPERATION, "produce")
      .setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, "topic")
      .setAttribute(AttributeKey.stringKey("messaging.kafka.topic"), record.topic())
//      .addLink(parentSpan.getSpanContext())
      .setParent(context)
      .startSpan();

    onStartNewSpan.accept(parentSpan);

    return newSpan;
  }

  public static <K, V> Span from(final Context context, final Tracer tracer, final KafkaConsumerRecord<K, V> record) {
    return from(context, tracer, record.topic() + " process", record);
  }

  public static <K, V> Span from(
    final Context context,
    final Tracer tracer,
    final String spanName,
    final KafkaConsumerRecord<K, V> record) {

    final var startMs = System.currentTimeMillis();

    return tracer.spanBuilder(spanName)
      .setStartTimestamp(startMs)
      .setSpanKind(Kind.CONSUMER)
      .setAttribute(SemanticAttributes.SERVICE_NAME, SERVICE_NAME)
      .setAttribute(SemanticAttributes.SERVICE_NAMESPACE, SERVICE_NAMESPACE)
      .setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "kafka")
      .setAttribute(SemanticAttributes.MESSAGING_OPERATION, "consume")
      .setAttribute(AttributeKey.stringKey("messaging.kafka.topic"), record.topic())
      .setAttribute(SemanticAttributes.MESSAGING_KAFKA_PARTITION, (long) record.partition())
      .setAttribute(AttributeKey.longKey("messaging.kafka.offset"), record.offset())
      .setAttribute(SemanticAttributes.MESSAGING_KAFKA_MESSAGE_KEY, messageKeyAttributeValue(record.key()))
      .setAttribute(SemanticAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES, serializedValueSize(record))
      .setAttribute("kafka.record.latency", latencyTime(startMs, record))
      .setParent(context)
      .startSpan();
  }

  public static void setEventAttributes(final Span span, final CloudEvent event) {
    if (event == null || span == null) {
      return;
    }

    span
      .setAttribute("cloudevent.id", event.getId())
      .setAttribute("cloudevent.source", event.getSource().toString())
      .setAttribute("cloudevent.type", event.getType())
      .setAttribute("cloudevent.specversion", event.getSpecVersion().toString());
  }

  private static <V, K> long latencyTime(long startMs, KafkaConsumerRecord<K, V> record) {
    if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
      return 0;
    }

    return Math.max(0L, startMs - record.timestamp());
  }

  private static <K> String messageKeyAttributeValue(final K key) {
    return key != null ? key.toString() : "null";
  }

  private static <K, V> long serializedValueSize(KafkaConsumerRecord<K, V> record) {
    return record.record().serializedValueSize();
  }

  public static String fromEnvOrDefault(final String key, final String defaultValue) {
    final var v = System.getenv(key);

    if (v == null || v.isEmpty()) {
      return defaultValue;
    }

    return v;
  }

  public static Span newClientChild(
    final Context context,
    final Tracer tracer,
    final String name,
    final Span span,
    Consumer<Span> onStartNewSpan) {

    final var newSpan = tracer.spanBuilder(name)
      .setSpanKind(Kind.CLIENT)
      .setAttribute(SemanticAttributes.SERVICE_NAME, SERVICE_NAME)
      .setAttribute(SemanticAttributes.SERVICE_NAMESPACE, SERVICE_NAMESPACE)
//      .addLink(span.getSpanContext())
      .setParent(context)
      .startSpan();

    onStartNewSpan.accept(span);

    return newSpan;
  }
}
