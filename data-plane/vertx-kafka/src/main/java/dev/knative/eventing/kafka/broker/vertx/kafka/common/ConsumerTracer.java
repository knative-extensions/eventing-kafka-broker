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

/*
 * Copied from https://github.com/vert-x3/vertx-kafka-client
 *
 * Copyright 2016 Red Hat Inc.
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
package dev.knative.eventing.kafka.broker.vertx.kafka.common;

import io.vertx.core.Context;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingPolicy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Utils;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * Tracer for Kafka consumer, wrapping the generic tracer.
 */
public class ConsumerTracer<S> {
  private final VertxTracer<S, Void> tracer;
  private final String address;
  private final String hostname;
  private final String port;
  private final TracingPolicy policy;

  /**
   * Creates a ConsumerTracer, which provides an opinionated facade for using {@link VertxTracer}
   * with a Kafka Consumer use case.
   * The method will return {@code null} if Tracing is not setup in Vert.x, or if {@code TracingPolicy.IGNORE} is used.
   * @param tracer the generic tracer object
   * @param opts Kafka client options
   * @param <S> the type of spans that is going to be generated, depending on the tracing system (zipkin, opentracing ...)
   * @return a new instance of {@code ConsumerTracer}, or {@code null}
   */
  public static <S> ConsumerTracer create(VertxTracer tracer, KafkaClientOptions opts) {
    TracingPolicy policy = opts.getTracingPolicy() != null ? opts.getTracingPolicy() : TracingPolicy.ALWAYS;
    if (policy == TracingPolicy.IGNORE || tracer == null) {
      // No need to create a tracer if it won't be used
      return null;
    }
    String address = opts.getTracePeerAddress();
    // Search for peer address in config if not provided
    if (address == null) {
      if (opts.getConfig() != null) {
        address = (String) opts.getConfig().getOrDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
      } else {
        address = "";
      }
    }
    return new ConsumerTracer<S>(tracer, policy, address);
  }

  private ConsumerTracer(VertxTracer<S, Void> tracer, TracingPolicy policy, String bootstrapServer) {
    this.tracer = tracer;
    this.address = bootstrapServer;
    this.hostname = Utils.getHost(bootstrapServer);
    Integer port = Utils.getPort(bootstrapServer);
    this.port = port == null ? null : port.toString();
    this.policy = policy;
  }

  private static Iterable<Map.Entry<String, String>> convertHeaders(Headers headers) {
    if (headers == null) {
      return Collections.emptyList();
    }
    return () -> StreamSupport.stream(headers.spliterator(), false)
      .map(h -> (Map.Entry<String, String>) new AbstractMap.SimpleEntry<>(h.key(), new String(h.value()))).iterator();
  }

  public StartedSpan prepareMessageReceived(Context context, ConsumerRecord rec) {
    TraceContext tc = new TraceContext("consumer", address, hostname, port, rec.topic());
    S span = tracer.receiveRequest(context, SpanKind.MESSAGING, policy, tc, "kafka_receive", convertHeaders(rec.headers()), TraceTags.TAG_EXTRACTOR);
    return new StartedSpan(span);
  }

  public class StartedSpan {
    private final S span;

    private StartedSpan(S span) {
      this.span = span;
    }

    public void finish(Context context) {
      // We don't add any new tag to the span here, just stop span timer
      tracer.sendResponse(context, null, span, null, TagExtractor.empty());
    }

    public void fail(Context context, Throwable failure) {
      tracer.sendResponse(context, null, span, failure, TagExtractor.empty());
    }
  }
}
