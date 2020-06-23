package dev.knative.eventing.kafka.broker.receiver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import org.junit.jupiter.api.Test;

public class RequestHandlerTest {

  @Test
  public void shouldSendRecordAndTerminateRequestWithRecordProduced() {
    shouldSendRecord(false, RequestHandler.RECORD_PRODUCED);
  }

  @Test
  public void shouldSendRecordAndTerminateRequestWithFailedToProduce() {
    shouldSendRecord(true, RequestHandler.FAILED_TO_PRODUCE);
  }

  @SuppressWarnings("unchecked")
  private static void shouldSendRecord(boolean failedToSend, int statusCode) {
    final var record = new KafkaProducerRecordImpl<>(
        "topic", "key", "value", 10
    );

    final RequestToRecordMapper<String, String> mapper
        = request -> Future.succeededFuture(record);

    final KafkaProducer<String, String> producer = mock(KafkaProducer.class);

    when(producer.send(any(), any())).thenAnswer(invocationOnMock -> {

      final var handler = (Handler<AsyncResult<RecordMetadata>>) invocationOnMock
          .getArgument(1, Handler.class);
      final var result = mock(AsyncResult.class);
      when(result.failed()).thenReturn(failedToSend);
      when(result.succeeded()).thenReturn(!failedToSend);

      handler.handle(result);

      return producer;
    });

    final var request = mock(HttpServerRequest.class);
    final var response = mockResponse(request, statusCode);

    final var handler = new RequestHandler<>(producer, mapper);
    handler.handle(request);

    verifySetStatusCodeAndTerminateResponse(statusCode, response);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void shouldReturnBadRequestIfNoRecordCanBeCreated() {
    final var producer = mock(KafkaProducer.class);

    final RequestToRecordMapper<Object, Object> mapper
        = (request) -> Future.failedFuture("");

    final var request = mock(HttpServerRequest.class);
    final var response = mockResponse(request, RequestHandler.MAPPER_FAILED);

    final var handler = new RequestHandler<Object, Object>(producer, mapper);
    handler.handle(request);

    verifySetStatusCodeAndTerminateResponse(RequestHandler.MAPPER_FAILED, response);
  }

  private static void verifySetStatusCodeAndTerminateResponse(
      final int statusCode,
      final HttpServerResponse response) {
    verify(response, times(1)).setStatusCode(statusCode);
    verify(response, times(1)).end();
  }

  private static HttpServerResponse mockResponse(
      final HttpServerRequest request,
      final int statusCode) {

    final var response = mock(HttpServerResponse.class);
    when(response.setStatusCode(statusCode)).thenReturn(response);

    when(request.response()).thenReturn(response);
    return response;
  }
}