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

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExactTokenMatcherTest {
    @Test
    void match() {
        ExactTokenMatcher matcher = new ExactTokenMatcher(Map.of("key", "val"));

        assertTrue(matcher.match(Map.of("key", List.of("val"))));
    }

    @Test
    void doNotMatchWithWrongValue() {
        ExactTokenMatcher matcher = new ExactTokenMatcher(Map.of("key", "val"));

        assertFalse(matcher.match(Map.of("key", List.of("value"))));
    }

    @Test
    void matchMultiple() {
        ExactTokenMatcher matcher = new ExactTokenMatcher(Map.of("key1", "val1", "key2", "val2"));

        assertTrue(matcher.match(Map.of("key1", List.of("val1"), "key2", List.of("val2"))));
        assertFalse(matcher.match(Map.of("key1", List.of("val2"), "key2", List.of("val1"))));
    }

    @Test
    void matchAnyOfSameKey() {
        ExactTokenMatcher matcher = new ExactTokenMatcher(Map.of("key1", "val1"));

        assertTrue(matcher.match(Map.of("key1", List.of("val1", "val2"))));
    }

    @Test
    void matchFailAsSoonAsAnyOfRequiredClaimsIsNotMatching() {
        ExactTokenMatcher matcher = new ExactTokenMatcher(Map.of("key1", "val1", "key2", "val2"));

        assertFalse(matcher.match(Map.of("key1", List.of("val1"))));
        assertFalse(matcher.match(Map.of("key1", List.of("val1"), "key2", List.of("foobar"))));
    }
}
