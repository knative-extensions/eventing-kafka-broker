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

public final class ControlPlaneProbeRequestUtil {

  private ControlPlaneProbeRequestUtil() {
  }

  // Control plane probing is based on the Knative Networking Data Plane Contract.
  // For a description of what these headers are see [1].
  //
  // [1]: https://github.com/knative/pkg/blob/b558677ab03404ed118ba3e179a7438fe2d8509a/network/network.go#L38-L51
  public static final String PROBE_HEADER_NAME = "K-Network-Probe";
  public static final String PROBE_HEADER_VALUE = "probe";
  public static final String PROBE_HASH_HEADER_NAME = "K-Network-Hash";

  /**
   * @param request HTTP request
   * @return true if the provided request conforms to the Knative Networking Data Plane Contract, false otherwise.
   */
  public static boolean isControlPlaneProbeRequest(final HttpServerRequest request) {
    final var headers = request.headers();
    return HttpMethod.GET.equals(request.method()) &&
      headers.contains(PROBE_HEADER_NAME) &&
      headers.get(PROBE_HEADER_NAME).equals(PROBE_HEADER_VALUE);
  }
}
