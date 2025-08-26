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
 * https://github.com/vert-x3/vertx-kafka-client/blob/a0e349fca33d3bb4f003ac53e6e0def42a76e8ab/src/main/java/io/vertx/kafka/client/common/tracing/ProducerTracer.java
 */
package dev.knative.eventing.kafka.broker.core.observability.tracing.kafka;

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
     * with a Kafka Producer use case. The method will return {@code null} if Tracing is not setup in Vert.x.
     * {@code TracingPolicy} is always set to {@code TracingPolicy.ALWAYS}.
     * @param tracer the generic tracer object
     */
    public ProducerTracer(VertxTracer<Void, S> tracer, TracingPolicy policy, String bootstrapServer) {
        this.tracer = tracer;
        this.address = bootstrapServer;
        this.hostname = Utils.getHost(bootstrapServer);
        final var port = Utils.getPort(bootstrapServer);
        this.port = port == null ? null : port.toString();
        this.policy = policy;
    }

    public StartedSpan prepareSendMessage(Context context, ProducerRecord<?, ?> record) {
        final var tc = new TraceContext("producer", address, hostname, port, record.topic());
        final var span = tracer.sendRequest(
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
