package dev.knative.eventing.kafka.broker.receiver.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public abstract class PreHandlerTest {

  private static final int PORT = 43999;
  protected static final int NEXT_HANDLER_STATUS_CODE = HttpResponseStatus.SERVICE_UNAVAILABLE.code();

  private WebClient webClient;
  private HttpServer server;

  @BeforeEach
  public void setUp(final Vertx vertx, final VertxTestContext context) {
    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    httpServerOptions.setHost("localhost");

    this.webClient = WebClient.create(vertx);

    Handler<HttpServerRequest> handler = createHandler();

    this.server = vertx.createHttpServer(httpServerOptions);
    this.server.requestHandler(request -> {
      handler.handle(request);

      if (!request.isEnded()) {
        request.response().setStatusCode(NEXT_HANDLER_STATUS_CODE).end();
      }
    })
      .listen(httpServerOptions.getPort(), httpServerOptions.getHost())
      .onComplete(context.succeedingThenComplete());
  }

  @AfterEach
  public void tearDown(final VertxTestContext context) {
    this.webClient.close();
    this.server.close().onComplete(context.succeedingThenComplete());
  }

  protected void mustReceiveStatusCodeOnPath(
    final VertxTestContext context,
    final int expectedStatusCode,
    final String path) {
    webClient.get(PORT, "localhost", path)
      .send()
      .onSuccess(response -> context.verify(() -> {
        assertThat(response.statusCode())
          .isEqualTo(expectedStatusCode);
        context.completeNow();
      }))
      .onFailure(context::failNow);
  }

  public abstract Handler<HttpServerRequest> createHandler();

}
