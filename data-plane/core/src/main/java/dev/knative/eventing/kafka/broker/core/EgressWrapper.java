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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.cloudevents.CloudEvent;
import java.util.Objects;

/**
 * TriggerWrapper wraps a Trigger for implementing the Trigger interface.
 *
 * <p>The wrapped Trigger Trigger must not be modified by callers.
 */
public class EgressWrapper implements Egress {

  private final DataPlaneContract.Egress egress;

  /**
   * All args constructor.
   *
   * @param egress trigger (it must not be modified by callers)
   */
  public EgressWrapper(final DataPlaneContract.Egress egress) {
    this.egress = egress;
  }

  @Override
  public String consumerGroup() {
    return egress.getConsumerGroup();
  }

  @Override
  public String destination() {
    return egress.getDestination();
  }

  @Override
  public boolean isReplyToUrl() {
    return egress.getReplyStrategyCase() == DataPlaneContract.Egress.ReplyStrategyCase.REPLYURL;
  }

  @Override
  public String replyUrl() {
    return egress.getReplyUrl();
  }

  @Override
  public boolean isReplyToOriginalTopic() {
    return egress.getReplyStrategyCase() == DataPlaneContract.Egress.ReplyStrategyCase.REPLYTOORIGINALTOPIC;
  }

  @Override
  public Filter<CloudEvent> filter() {
    return egress.getFilter() != null ? new EventMatcher(egress.getFilter().getAttributesMap()) : Filter.noopMatcher();
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof EgressWrapper)) {
      return false;
    }
    final var that = (EgressWrapper) object;
    return Objects.equals(this.consumerGroup(), that.consumerGroup())
      && Objects.equals(this.destination(), that.destination())
      && Objects.equals(this.isReplyToUrl(), that.isReplyToUrl())
      && Objects.equals(this.isReplyToOriginalTopic(), that.isReplyToOriginalTopic())
      && Objects.equals(this.replyUrl(), that.replyUrl())
      && Objects.equals(this.egress.getFilter(), that.egress.getFilter());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      consumerGroup(),
      destination(),
      isReplyToUrl(),
      isReplyToOriginalTopic(),
      replyUrl(),
      egress.getFilter()
    );
  }

  @Override
  public String toString() {
    return "EgressWrapper{"
      + "egress=" + egress
      + '}';
  }
}
