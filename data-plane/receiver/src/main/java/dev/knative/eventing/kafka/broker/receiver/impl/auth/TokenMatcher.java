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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface TokenMatcher {
    boolean match(Map<String, List<String>> claims);

    static List<TokenMatcher> fromContract(List<DataPlaneContract.TokenMatcher> contractTokenMatchers) {
        List<TokenMatcher> matchers = new ArrayList<>(contractTokenMatchers.size());

        for (var contractTokenMatcher : contractTokenMatchers) {
            switch (contractTokenMatcher.getMatcherCase()) {
                case EXACT -> matchers.add(ExactTokenMatcher.fromContract(contractTokenMatcher.getExact()));
                case PREFIX -> matchers.add(PrefixTokenMatcher.fromContract(contractTokenMatcher.getPrefix()));
            }
        }

        return matchers;
    }
}
