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
package dev.knative.eventing.kafka.broker.core.filter;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi.*;
import io.cloudevents.CloudEvent;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This interface provides an abstraction for filtering {@link CloudEvent} instances.
 */
@FunctionalInterface
public interface Filter extends Predicate<CloudEvent> {

    /**
     * @return noop implementation that always returns true
     */
    static Filter noop() {
        return ce -> true;
    }

    static Filter fromContract(DataPlaneContract.DialectedFilter filter) {
        return switch (filter.getFilterCase()) {
            case EXACT -> new ExactFilter(filter.getExact().getAttributesMap());
            case PREFIX -> new PrefixFilter(filter.getPrefix().getAttributesMap());
            case SUFFIX -> new SuffixFilter(filter.getSuffix().getAttributesMap());
            case NOT -> new NotFilter(fromContract(filter.getNot().getFilter()));
            case ANY ->
                AnyFilter.newFilter(filter.getAny().getFiltersList().stream()
                        .map(Filter::fromContract)
                        .collect(Collectors.toList()));
            case ALL ->
                AllFilter.newFilter(filter.getAll().getFiltersList().stream()
                        .map(Filter::fromContract)
                        .collect(Collectors.toList()));
            case CESQL -> new CeSqlFilter(filter.getCesql().getExpression());
            default -> Filter.noop();
        };
    }

    static Filter fromContract(List<DataPlaneContract.DialectedFilter> filters) {
        return AllFilter.newFilter(filters.stream().map(Filter::fromContract).collect(Collectors.toList()));
    }
}
