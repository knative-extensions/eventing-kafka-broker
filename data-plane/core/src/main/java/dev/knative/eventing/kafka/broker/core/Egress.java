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

package dev.knative.eventing.kafka.broker.core;

import io.cloudevents.CloudEvent;

/**
 * Egress interface represents the Egress object.
 *
 * <p>Each implementation must override: equals(object) and hashCode(), and those implementation
 * must catch Egress updates (e.g. it's not safe to compare only the Egress UID). It's recommended
 * to not relying on equals(object) and hashCode() generated by Protocol Buffer compiler.
 *
 * <p>Testing equals(object) and hashCode() of newly added implementation is done by adding sources
 * to parameterized tests in EgressTest.
 */
public interface Egress {

  /**
   * Get egress id.
   *
   * @return egress identifier.
   */
  String consumerGroup();

  /**
   * Get egress destination URI.
   *
   * @return destination URI.
   */
  String destination();

  boolean isReplyToUrl();

  String replyUrl();

  boolean isReplyToOriginalTopic();

  /**
   * Get the filter.
   *
   * @return filter to use.
   */
  Filter<CloudEvent> filter();
}
