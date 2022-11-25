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

/*
 * Copied from https://github.com/vert-x3/vertx-kafka-client
 *
 * Copyright 2016 Red Hat Inc.
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
package dev.knative.eventing.kafka.broker.vertx.kafka.common.impl;

import dev.knative.eventing.kafka.broker.vertx.kafka.common.TopicPartition;
import dev.knative.eventing.kafka.broker.vertx.kafka.consumer.OffsetAndMetadata;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.RecordMetadata;
import io.vertx.core.Handler;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class for mapping native and Vert.x Kafka objects
 */
public class Helper {

  private Helper() {
  }

  public static <T> Set<T> toSet(Collection<T> collection) {
    if (collection instanceof Set) {
      return (Set<T>) collection;
    } else {
      return new HashSet<>(collection);
    }
  }

  public static org.apache.kafka.common.TopicPartition to(TopicPartition topicPartition) {
    return new org.apache.kafka.common.TopicPartition(topicPartition.getTopic(), topicPartition.getPartition());
  }

  public static Set<org.apache.kafka.common.TopicPartition> to(Set<TopicPartition> topicPartitions) {
    return topicPartitions.stream().map(Helper::to).collect(Collectors.toSet());
  }

  public static Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> to(Map<TopicPartition, OffsetAndMetadata> offsets) {
    return offsets.entrySet().stream().collect(Collectors.toMap(
      e -> new org.apache.kafka.common.TopicPartition(e.getKey().getTopic(), e.getKey().getPartition()),
      e -> new org.apache.kafka.clients.consumer.OffsetAndMetadata(e.getValue().getOffset(), e.getValue().getMetadata()))
    );
  }

  public static Map<TopicPartition, OffsetAndMetadata> from(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
    return offsets.entrySet().stream().collect(Collectors.toMap(
      e -> new TopicPartition(e.getKey().topic(), e.getKey().partition()),
      e -> new OffsetAndMetadata(e.getValue().offset(), e.getValue().metadata()))
    );
  }

  public static TopicPartition from(org.apache.kafka.common.TopicPartition topicPartition) {
    return new TopicPartition(topicPartition.topic(), topicPartition.partition());
  }

  public static Set<TopicPartition> from(Collection<org.apache.kafka.common.TopicPartition> topicPartitions) {
    return topicPartitions.stream().map(Helper::from).collect(Collectors.toSet());
  }

  public static Handler<Set<org.apache.kafka.common.TopicPartition>> adaptHandler(Handler<Set<TopicPartition>> handler) {
    if (handler != null) {
      return topicPartitions -> handler.handle(Helper.from(topicPartitions));
    } else {
      return null;
    }
  }

  public static RecordMetadata from(org.apache.kafka.clients.producer.RecordMetadata metadata) {
    return new RecordMetadata(metadata.offset(),
      metadata.partition(), metadata.timestamp(), metadata.topic());
  }

  public static OffsetAndMetadata from(org.apache.kafka.clients.consumer.OffsetAndMetadata offsetAndMetadata) {
    if (offsetAndMetadata != null) {
      return new OffsetAndMetadata(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
    } else {
      return null;
    }
  }

  public static org.apache.kafka.clients.consumer.OffsetAndMetadata to(OffsetAndMetadata offsetAndMetadata) {
    return new org.apache.kafka.clients.consumer.OffsetAndMetadata(offsetAndMetadata.getOffset(), offsetAndMetadata.getMetadata());
  }
}
