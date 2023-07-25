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
package dev.knative.eventing.kafka.broker.core.tracing.kafka;

import io.vertx.core.Context;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingPolicy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;

/**
 * Tracer for Kafka producer, wrapping the generic tracer.
 */
public class ProducerTracer<S> {
    private final VertxTracer<Void, S> tracer;
    private final String address;
    private final String hostname;
    private final String port;
    private final TracingPolicy policy;

    /**
     * Creates a ProducerTracer, which provides an opinionated facade for using {@link io.vertx.core.spi.tracing.VertxTracer}
     * with a Kafka Producer use case.
     * The method will return {@code null} if Tracing is not setup in Vert.x, or if {@code TracingPolicy.IGNORE} is used.
     * @param tracer the generic tracer object
     * @param opts Kafka client options
     * @param <S> the type of spans that is going to be generated, depending on the tracing system (zipkin, opentracing ...)
     * @return a new instance of {@code ProducerTracer}, or {@code null}
     */
    public static <S> ProducerTracer create(VertxTracer tracer) {
        TracingPolicy policy = TracingPolicy.ALWAYS;
        return new ProducerTracer<S>(tracer, policy, "");
    }

    private ProducerTracer(VertxTracer<Void, S> tracer, TracingPolicy policy, String bootstrapServer) {
        this.tracer = tracer;
        this.address = bootstrapServer;
        this.hostname = Utils.getHost(bootstrapServer);
        Integer port = Utils.getPort(bootstrapServer);
        this.port = port == null ? null : port.toString();
        this.policy = policy;
    }

    public StartedSpan prepareSendMessage(Context context, ProducerRecord record) {
        TraceContext tc = new TraceContext("producer", address, hostname, port, record.topic());
        S span = tracer.sendRequest(
                context,
                SpanKind.MESSAGING,
                policy,
                tc,
                "kafka_send",
                (k, v) -> record.headers().add(k, v.getBytes()),
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
            tracer.receiveResponse(context, null, span, null, TagExtractor.<TraceContext>empty());
        }

        public void fail(Context context, Throwable failure) {
            tracer.receiveResponse(context, null, span, failure, TagExtractor.empty());
        }
    }
}
