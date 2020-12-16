/*
 * Copyright 2020 The Knative Authors
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
package dev.knative.eventing.kafka.broker.dispatcher;

import static java.util.Objects.requireNonNull;

import dev.knative.eventing.kafka.broker.core.utils.BaseEnv;
import java.util.function.Function;

public class DispatcherEnv extends BaseEnv {

  public static final String CONSUMER_CONFIG_FILE_PATH = "CONSUMER_CONFIG_FILE_PATH";
  public static final String WEBCLIENT_CONFIG_FILE_PATH = "WEBCLIENT_CONFIG_FILE_PATH";
  public static final String EGRESSES_INITIAL_CAPACITY = "EGRESSES_INITIAL_CAPACITY";

  private final String consumerConfigFilePath;
  private final String webClientConfigFilePath;
  private final int egressesInitialCapacity;

  public DispatcherEnv(final Function<String, String> envProvider) {
    super(envProvider);

    this.consumerConfigFilePath = requireNonNull(envProvider.apply(CONSUMER_CONFIG_FILE_PATH));
    this.webClientConfigFilePath = requireNonNull(envProvider.apply(WEBCLIENT_CONFIG_FILE_PATH));
    this.egressesInitialCapacity =
        Integer.parseInt(requireNonNull(envProvider.apply(EGRESSES_INITIAL_CAPACITY)));
  }

  public String getConsumerConfigFilePath() {
    return consumerConfigFilePath;
  }

  public String getWebClientConfigFilePath() {
    return webClientConfigFilePath;
  }

  public int getEgressesInitialCapacity() {
    return egressesInitialCapacity;
  }

  @Override
  public String toString() {
    return "DispatcherEnv{"
        + "consumerConfigFilePath='"
        + consumerConfigFilePath
        + '\''
        + ", webClientConfigFilePath='"
        + webClientConfigFilePath
        + '\''
        + ", egressesInitialCapacity="
        + egressesInitialCapacity
        + "} "
        + super.toString();
  }
}
