package dev.knative.eventing.kafka.broker.receiver.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.receiver.HttpVerticle;
import dev.knative.eventing.kafka.broker.receiver.SimpleProbeHandlerDecorator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ProbeHandlerTest {

  private static final int PORT = 43999;

  private static final String LIVENESS_PATH = "/healthz";
  private static final String READINESS_PATH = "/readyz";
  private static final int OK = HttpResponseStatus.OK.code();
  private static final int NEXT_HANDLER_STATUS_CODE = HttpResponseStatus.SERVICE_UNAVAILABLE.code();

  private static HttpClient httpClient;

  @BeforeEach
  public void setUp(final Vertx vertx, final VertxTestContext context) {
    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    httpServerOptions.setHost("localhost");
    final var verticle = new HttpVerticle(httpServerOptions, new SimpleProbeHandlerDecorator(
        LIVENESS_PATH,
        READINESS_PATH,
        r -> r.response().setStatusCode(NEXT_HANDLER_STATUS_CODE).end()
    ));
    httpClient = vertx.createHttpClient();
    vertx.deployVerticle(verticle, context.succeeding(ar -> context.completeNow()));
  }

  @AfterAll
  public static void tearDown() {
    httpClient.close();
  }

  @Test
  public void testReadinessCheck(final VertxTestContext context) {
    mustReceiveStatusCodeOnPath(context, OK, READINESS_PATH);
  }

  @Test
  public void testLivenessCheck(final VertxTestContext context) {
    mustReceiveStatusCodeOnPath(context, OK, LIVENESS_PATH);
  }

  @Test
  public void shouldForwardToNextHandler(final VertxTestContext context) {
    mustReceiveStatusCodeOnPath(context, NEXT_HANDLER_STATUS_CODE, "/does-not-exists-42");
  }

  private static void mustReceiveStatusCodeOnPath(
      final VertxTestContext context,
      final int expectedStatusCode,
      final String path) {
    doRequest(path)
        .onSuccess(statusCode -> context.verify(() -> {
          assertThat(statusCode).isEqualTo(expectedStatusCode);
          context.completeNow();
        }))
        .onFailure(context::failNow);
  }

  private static Future<Integer> doRequest(final String path) {
    final Promise<Integer> promise = Promise.promise();

    httpClient.get(PORT, "localhost", path)
        .exceptionHandler(promise::tryFail)
        .handler(response -> promise.tryComplete(response.statusCode()))
        .end();

    return promise.future();
  }
}
