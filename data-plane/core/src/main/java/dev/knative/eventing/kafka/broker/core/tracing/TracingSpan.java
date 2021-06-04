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
package dev.knative.eventing.kafka.broker.core.tracing;

import io.cloudevents.CloudEvent;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

public class TracingSpan {
  private static final AttributeKey<String> MESSAGING_MESSAGE_ID = stringKey("messaging.message_id");
  private static final AttributeKey<String> MESSAGING_MESSAGE_SOURCE = stringKey("messaging.message_source");
  private static final AttributeKey<String> MESSAGING_MESSAGE_TYPE = stringKey("messaging.message_type");

  public static void decorateCurrentWithEvent(final CloudEvent event) {
    Span span = Span.fromContextOrNull(Context.current());
    if (span == null) {
      return;
    }
    span.setAttribute(MESSAGING_MESSAGE_ID, event.getId());
    span.setAttribute(MESSAGING_MESSAGE_SOURCE, event.getSource().toString());
    span.setAttribute(MESSAGING_MESSAGE_TYPE, event.getType());
  }
}
