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
package dev.knative.eventing.kafka.broker.core.utils;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread unsafe holder with reference counter.
 *
 * @param <T> the type of the value to hold
 */
public class ReferenceCounter<T> {

    private final T value;
    private final AtomicInteger refs;

    public ReferenceCounter(final T value) {
        Objects.requireNonNull(value);
        this.value = value;
        this.refs = new AtomicInteger(0);
    }

    /**
     * @return the inner value
     */
    public T getValue() {
        return value;
    }

    /**
     * Increment the ref count
     */
    public void increment() {
        this.refs.incrementAndGet();
    }

    /**
     * @return true if the count is 0, hence nobody is referring anymore to this value
     */
    public boolean decrementAndCheck() {
        return this.refs.decrementAndGet() == 0;
    }
}
