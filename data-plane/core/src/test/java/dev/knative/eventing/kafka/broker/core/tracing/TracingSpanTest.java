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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cloudevents.core.builder.CloudEventBuilder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class TracingSpanTest {

  @Test
  public void shouldDecorateCurrentSpan(final Vertx vertx) throws InterruptedException {

    final var ctx = vertx.getOrCreateContext();

    final var span = mock(Span.class);
    when(span.setAttribute(any(AttributeKey.class), any())).thenReturn(span);

    ctx.putLocal(TracingSpan.ACTIVE_SPAN, span);

    final var wait = new CountDownLatch(1);

    ctx.runOnContext(ignored -> {

      TracingSpan.decorateCurrent(vertx, CloudEventBuilder.v1()
        .withSource(URI.create("/hello"))
        .withType("type")
        .withId(UUID.randomUUID().toString())
        .build());

      wait.countDown();
    });

    wait.await(1, TimeUnit.SECONDS);

    verify(span, atLeastOnce()).setAttribute(any(AttributeKey.class), any());
  }

  @Test
  public void shouldReturnCurrentSpan(final Vertx vertx, final VertxTestContext context) throws InterruptedException {

    final var ctx = vertx.getOrCreateContext();
    final var currentSpan = mock(Span.class);
    ctx.putLocal(TracingSpan.ACTIVE_SPAN, currentSpan);

    ctx.runOnContext(ignored -> {

      final var span = TracingSpan.getCurrent(vertx);

      assertThat(span).isSameAs(span);

      context.completeNow();
    });
  }
}
