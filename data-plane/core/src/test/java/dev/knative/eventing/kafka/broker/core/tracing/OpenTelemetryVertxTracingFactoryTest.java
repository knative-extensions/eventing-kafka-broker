package dev.knative.eventing.kafka.broker.core.tracing;

import static dev.knative.eventing.kafka.broker.core.tracing.TracingSpan.ACTIVE_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.vertx.core.Vertx;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.junit5.VertxExtension;
import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class OpenTelemetryVertxTracingFactoryTest {

  @Test
  public void receiveRequestShouldNotReturnSpanIfPolicyIsIgnore(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final var span = tracer.receiveRequest(
      vertx.getOrCreateContext(),
      SpanKind.MESSAGING,
      TracingPolicy.IGNORE,
      null,
      "",
      Collections.emptyList(),
      new TagExtractor<>() {
      }
    );

    assertThat(span).isNull();
  }

  @Test
  public void receiveRequestShouldNotReturnSpanIfPolicyIsPropagateAndPreviousContextIsNotPresent(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final var span = tracer.receiveRequest(
      vertx.getOrCreateContext(),
      SpanKind.MESSAGING,
      TracingPolicy.PROPAGATE,
      null,
      "",
      Collections.emptyList(),
      new TagExtractor<>() {
      }
    );

    assertThat(span).isNull();
  }

  @Test
  public void receiveRequestShouldReturnSpanIfPolicyIsPropagateAndPreviousContextIsPresent(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final Iterable<Map.Entry<String, String>> headers = Collections.singletonList(
      new SimpleImmutableEntry<>("traceparent", "00-83ebbd06a32c2eaa8d5bf4b060d7cbfa-140cd1a04ab7be4b-01")
    );

    final var ctx = vertx.getOrCreateContext();
    final var span = tracer.receiveRequest(
      ctx,
      SpanKind.MESSAGING,
      TracingPolicy.PROPAGATE,
      null,
      "",
      headers,
      new TagExtractor<>() {
      }
    );

    assertThat(span).isNotNull();

    final var tracingContext = ctx.getLocal(ACTIVE_CONTEXT);
    assertThat(tracingContext).isNotNull();
  }

  @Test
  public void sendResponseEndsSpan(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final var span = mock(Span.class);
    doNothing().when(span).end();

    tracer.sendResponse(
      vertx.getOrCreateContext(),
      mock(Serializable.class),
      span,
      mock(Exception.class),
      new TagExtractor<>() {
      }
    );

    verify(span, times(1)).end();
  }

  @Test
  public void sendResponseShouldNotThrowExceptionWhenSpanIsNull(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    assertThatNoException().isThrownBy(() -> tracer.sendResponse(
      vertx.getOrCreateContext(),
      mock(Serializable.class),
      null,
      mock(Exception.class),
      new TagExtractor<>() {
      }
    ));
  }

  @Test
  public void sendRequestShouldNotReturnSpanIfRequestIsNull(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final var ctx = vertx.getOrCreateContext();
    ctx.putLocal(ACTIVE_CONTEXT, Context.current());

    final var span = tracer.sendRequest(
      ctx,
      SpanKind.MESSAGING,
      TracingPolicy.PROPAGATE,
      null,
      "",
      (k, v) -> {
      },
      new TagExtractor<>() {
      }
    );

    assertThat(span).isNull();
  }

  @Test
  public void sendRequestShouldNotReturnSpanIfPolicyIsIgnore(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final var ctx = vertx.getOrCreateContext();
    ctx.putLocal(ACTIVE_CONTEXT, Context.current());

    final var span = tracer.sendRequest(
      ctx,
      SpanKind.MESSAGING,
      TracingPolicy.IGNORE,
      mock(Serializable.class),
      "",
      (k, v) -> {
      },
      new TagExtractor<>() {
      }
    );

    assertThat(span).isNull();
  }


  @Test
  public void sendRequestShouldNotReturnSpanIfPolicyIsPropagateAndPreviousContextIsNotPresent(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final var span = tracer.sendRequest(
      vertx.getOrCreateContext(),
      SpanKind.MESSAGING,
      TracingPolicy.PROPAGATE,
      null,
      "",
      (k, v) -> {
      },
      new TagExtractor<>() {
      }
    );

    assertThat(span).isNull();
  }

  @Test
  public void sendRequestShouldReturnSpanIfPolicyIsPropagateAndPreviousContextIsPresent(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final var ctx = vertx.getOrCreateContext();
    ctx.putLocal(ACTIVE_CONTEXT, Context.current());

    final var span = tracer.sendRequest(
      ctx,
      SpanKind.MESSAGING,
      TracingPolicy.PROPAGATE,
      mock(Serializable.class),
      "",
      (k, v) -> {
      },
      new TagExtractor<>() {
      }
    );

    assertThat(span).isNotNull();
  }

  @Test
  public void sendRequestShouldReturnSpanIfPolicyIsAlwaysAndPreviousContextIsNotPresent(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final var ctx = vertx.getOrCreateContext();

    final var span = tracer.sendRequest(
      ctx,
      SpanKind.MESSAGING,
      TracingPolicy.ALWAYS,
      mock(Serializable.class),
      "",
      (k, v) -> {
      },
      new TagExtractor<>() {
      }
    );

    assertThat(span).isNotNull();
  }

  @Test
  public void receiveResponseEndsSpan(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    final var span = mock(Span.class);
    doNothing().when(span).end();

    tracer.receiveResponse(
      vertx.getOrCreateContext(),
      mock(Serializable.class),
      span,
      mock(Exception.class),
      new TagExtractor<>() {
      }
    );

    verify(span, times(1)).end();
  }

  @Test
  public void receiveResponseShouldNotThrowExceptionWhenSpanIsNull(final Vertx vertx) {

    final var f = new OpenTelemetryVertxTracingFactory(Tracer.getDefault());
    final var tracer = f.tracer(null);

    assertThatNoException().isThrownBy(() -> tracer.receiveResponse(
      vertx.getOrCreateContext(),
      mock(Serializable.class),
      null,
      mock(Exception.class),
      new TagExtractor<>() {
      }
    ));
  }
}
