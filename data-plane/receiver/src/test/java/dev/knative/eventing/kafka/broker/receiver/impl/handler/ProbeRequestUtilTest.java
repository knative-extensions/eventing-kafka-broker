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
package dev.knative.eventing.kafka.broker.receiver.impl.handler;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import org.junit.jupiter.api.Test;

import static dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil.PROBE_HEADER_NAME;
import static dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil.PROBE_HEADER_VALUE;
import static dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil.isControlPlaneProbeRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProbeRequestUtilTest {

  @Test
  public void shouldBeProbeRequest() {
    final var headers = new HeadersMultiMap()
      .add(PROBE_HEADER_NAME, PROBE_HEADER_VALUE);
    final var request = mock(HttpServerRequest.class);
    when(request.method()).thenReturn(HttpMethod.GET);
    when(request.headers()).thenReturn(headers);

    assertThat(isControlPlaneProbeRequest(request)).isTrue();
  }

  @Test
  public void shouldNotBeProbeRequestWrongValue() {
    final var headers = new HeadersMultiMap()
      .add(PROBE_HEADER_NAME, PROBE_HEADER_VALUE + "a");
    final var request = mock(HttpServerRequest.class);
    when(request.method()).thenReturn(HttpMethod.GET);
    when(request.headers()).thenReturn(headers);

    assertThat(isControlPlaneProbeRequest(request)).isFalse();
  }

  @Test
  public void shouldNotBeProbeRequestNoProbeHeader() {
    final var headers = new HeadersMultiMap();
    final var request = mock(HttpServerRequest.class);
    when(request.method()).thenReturn(HttpMethod.GET);
    when(request.headers()).thenReturn(headers);

    assertThat(isControlPlaneProbeRequest(request)).isFalse();
  }

  @Test
  public void shouldNotBeProbeRequestWrongMethod() {
    final var headers = new HeadersMultiMap();
    final var request = mock(HttpServerRequest.class);
    when(request.method()).thenReturn(HttpMethod.POST);
    when(request.headers()).thenReturn(headers);

    assertThat(isControlPlaneProbeRequest(request)).isFalse();
  }
}
