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
package dev.knative.eventing.kafka.broker.receiver.impl.auth;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EventPolicy {
    private final List<TokenMatcher> tokenMatchers;

    public static EventPolicy fromContract(DataPlaneContract.EventPolicy contractEventPolicy) {
        return new EventPolicy(TokenMatcher.fromContract(contractEventPolicy.getTokenMatchersList()));
    }

    public static List<EventPolicy> fromContract(List<DataPlaneContract.EventPolicy> contractEventPolicies) {
        return contractEventPolicies.stream().map(EventPolicy::fromContract).collect(Collectors.toList());
    }

    public EventPolicy(List<TokenMatcher> tokenMatchers) {
        this.tokenMatchers = tokenMatchers;
    }

    public boolean isAuthorized(Map<String, List<String>> claims) {
        for (TokenMatcher matcher : tokenMatchers) {
            if (matcher.match(claims)) {
                return true;
            }
        }

        return false;
    }
}
