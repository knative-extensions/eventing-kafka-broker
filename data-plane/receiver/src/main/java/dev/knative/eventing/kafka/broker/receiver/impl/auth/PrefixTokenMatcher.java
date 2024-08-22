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

class PrefixTokenMatcher implements TokenMatcher {
    private final Map<String, String> requiredMatches;

    public static PrefixTokenMatcher fromContract(DataPlaneContract.Prefix prefixTokenMatcher) {
        return new PrefixTokenMatcher(prefixTokenMatcher.getAttributesMap());
    }

    public PrefixTokenMatcher(Map<String, String> requiredMatches) {
        this.requiredMatches = requiredMatches;
    }

    @Override
    public boolean match(Map<String, List<String>> claims) {
        for (Map.Entry<String, String> requiredMatch : requiredMatches.entrySet()) {
            if (!claims.containsKey(requiredMatch.getKey())
                    || !claims.get(requiredMatch.getKey()).stream()
                            .anyMatch(s -> s.startsWith(requiredMatch.getValue()))) {
                // as soon as one of the required claims does not match, the matcher should fail
                return false;
            }
        }

        return true;
    }
}
