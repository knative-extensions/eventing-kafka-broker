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
package dev.knative.eventing.kafka.broker.dispatcher.impl;

import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.vertx.core.Future;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecordDispatcherMutatorChainTest {

  private final static CloudEvent expected = CloudEventBuilder.v1()
    .withId(UUID.randomUUID().toString())
    .withSource(URI.create("/v1/api"))
    .withTime(OffsetDateTime.MIN)
    .withType("foo")
    .build();

  @Test
  public void shouldChangeRecordValue() {
    final var next = mock(RecordDispatcher.class);
    final var chain = new RecordDispatcherMutatorChain(
      next,
      in -> expected
    );
    when(next.dispatch(any())).thenReturn(Future.succeededFuture());

    final var givenRecord = new KafkaConsumerRecordImpl<>(
      new ConsumerRecord<>(
        "t1",
        0,
        0,
        (Object) "abc",
        CloudEventBuilder.from(expected)
          .withId(UUID.randomUUID().toString())
          .build())
    );

    final var succeeded = chain.dispatch(givenRecord);

    assertThat(succeeded.succeeded()).isTrue();
    verify(next, times(1)).dispatch(argThat(record -> record.value().equals(expected)));
  }

  @Test
  public void shouldCloseInner() {
    final var next = mock(RecordDispatcher.class);
    final var chain = new RecordDispatcherMutatorChain(
      next,
      in -> expected
    );
    when(next.close()).thenReturn(Future.succeededFuture());

    final var succeeded = chain.close();

    assertThat(succeeded.succeeded()).isTrue();
    verify(next, times(1)).close();
  }
}
