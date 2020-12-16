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

import static io.opentelemetry.api.common.AttributeKey.stringKey;

import io.cloudevents.CloudEvent;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.attributes.SemanticAttributes;
import io.vertx.core.Vertx;

public class TracingSpan {

  static final String ACTIVE_CONTEXT = "tracing.context";
  static final String ACTIVE_SPAN = "tracing.span";

  public static final AttributeKey<String> MESSAGING_MESSAGE_SOURCE =
      stringKey("messaging.message_source");
  public static final AttributeKey<String> MESSAGING_MESSAGE_TYPE =
      stringKey("messaging.message_type");

  public static void decorateCurrent(final Vertx vertx, final CloudEvent event) {

    final var span = getCurrent(vertx);
    if (span == null) {
      return;
    }

    span.setAttribute(SemanticAttributes.MESSAGING_MESSAGE_ID, event.getId());
    span.setAttribute(MESSAGING_MESSAGE_SOURCE, event.getSource().toString());
    span.setAttribute(MESSAGING_MESSAGE_TYPE, event.getType());
  }

  public static Span getCurrent(final Vertx vertx) {
    if (vertx == null) {
      return null;
    }
    final var ctx = vertx.getOrCreateContext();
    if (ctx == null) {
      return null;
    }

    return ctx.getLocal(ACTIVE_SPAN);
  }
}
