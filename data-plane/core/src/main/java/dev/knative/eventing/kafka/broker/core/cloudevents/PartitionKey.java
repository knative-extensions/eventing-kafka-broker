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

package dev.knative.eventing.kafka.broker.core.cloudevents;

import io.cloudevents.CloudEvent;

public final class PartitionKey {

  public static final String PARTITION_KEY_KEY = "partitionkey";

  /**
   * Extract the partitionkey extension from the given event.
   *
   * @param event event from which extracting partition key.
   * @return partitionkey extension value or null if not set
   * @link https://github.com/cloudevents/spec/blob/master/extensions/partitioning.md
   */
  public static String extract(final CloudEvent event) {

    final var partitionKey = event.getExtension(PARTITION_KEY_KEY);
    if (partitionKey == null) {
      return null;
    }

    return partitionKey.toString();
  }
}
