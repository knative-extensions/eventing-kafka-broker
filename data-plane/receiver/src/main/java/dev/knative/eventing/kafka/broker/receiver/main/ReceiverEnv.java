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
package dev.knative.eventing.kafka.broker.receiver.main;

import dev.knative.eventing.kafka.broker.core.utils.BaseEnv;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

class ReceiverEnv extends BaseEnv {

  public static final String INGRESS_PORT = "INGRESS_PORT";
  private final int ingressPort;

  public static final String LIVENESS_PROBE_PATH = "LIVENESS_PROBE_PATH";
  private final String livenessProbePath;

  public static final String READINESS_PROBE_PATH = "READINESS_PROBE_PATH";
  private final String readinessProbePath;

  public static final String HTTPSERVER_CONFIG_FILE_PATH = "HTTPSERVER_CONFIG_FILE_PATH";
  private final String httpServerConfigFilePath;

  ReceiverEnv(final Function<String, String> envProvider) {
    super(envProvider);

    this.ingressPort = Integer.parseInt(envProvider.apply(INGRESS_PORT));
    this.livenessProbePath = requireNonNull(envProvider.apply(LIVENESS_PROBE_PATH));
    this.readinessProbePath = requireNonNull(envProvider.apply(READINESS_PROBE_PATH));
    this.httpServerConfigFilePath = requireNonNull(envProvider.apply(HTTPSERVER_CONFIG_FILE_PATH));
  }

  public int getIngressPort() {
    return ingressPort;
  }

  public String getLivenessProbePath() {
    return livenessProbePath;
  }

  public String getReadinessProbePath() {
    return readinessProbePath;
  }

  public String getHttpServerConfigFilePath() {
    return httpServerConfigFilePath;
  }

  @Override
  public String toString() {
    return "ReceiverEnv{" +
      "ingressPort=" + ingressPort +
      ", livenessProbePath='" + livenessProbePath + '\'' +
      ", readinessProbePath='" + readinessProbePath + '\'' +
      ", httpServerConfigFilePath='" + httpServerConfigFilePath + '\'' +
      "} " + super.toString();
  }
}
