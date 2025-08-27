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

/**
 * This piece of code is inspired from vert-x3/vertx-kafka-client project.
 * The original source code can be found here:
 * https://github.com/vert-x3/vertx-kafka-client/blob/a0e349fca33d3bb4f003ac53e6e0def42a76e8ab/src/main/java/io/vertx/kafka/client/common/tracing/ConsumerTracer.java
 */
package dev.knative.eventing.kafka.broker.core.observability.tracing.kafka;

import io.vertx.core.Context;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingPolicy;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Utils;

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
     * Creates a ConsumerTracer, which provides an opinionated facade for using {@link io.vertx.core.spi.tracing.VertxTracer}
     * with a Kafka Consumer use case.
     * The method will return {@code null} if Tracing is not setup in Vert.x.
     * {@code TracingPolicy} is always set to {@code TracingPolicy.ALWAYS}.
     * @param tracer the generic tracer object
     * @param config Kafka client configuration
     * @param <S> the type of spans that is going to be generated, depending on the tracing system (zipkin, opentracing ...)
     * @return a new instance of {@code ConsumerTracer}, or {@code null}
     */
    public static <S> ConsumerTracer create(VertxTracer tracer, Map<String, Object> config, TracingPolicy policy) {
        if (tracer == null) {
            return null;
        }
        policy = policy == null ? TracingPolicy.ALWAYS : policy;
        String address =
                config.getOrDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "").toString();
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
                .map(h -> (Map.Entry<String, String>) new AbstractMap.SimpleEntry<>(h.key(), new String(h.value())))
                .iterator();
    }

    public StartedSpan prepareMessageReceived(Context context, ConsumerRecord rec) {
        TraceContext tc = new TraceContext("consumer", address, hostname, port, rec.topic());
        S span = tracer.receiveRequest(
                context,
                SpanKind.MESSAGING,
                policy,
                tc,
                "kafka_receive",
                convertHeaders(rec.headers()),
                TraceTags.TAG_EXTRACTOR);
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
