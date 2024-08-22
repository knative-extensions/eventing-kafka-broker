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

class PrefixTokenMatcherTest {

    @Test
    void match() {
        PrefixTokenMatcher matcher = new PrefixTokenMatcher(Map.of("key", "prefix"));

        assertTrue(matcher.match(Map.of("key", List.of("prefix-foo"))));
    }

    @Test
    void doNotMatchWithWrongPrefix() {
        PrefixTokenMatcher matcher = new PrefixTokenMatcher(Map.of("key", "prefix"));

        assertFalse(matcher.match(Map.of("key", List.of("foo-bar"))));
    }

    @Test
    void matchMultiple() {
        PrefixTokenMatcher matcher = new PrefixTokenMatcher(Map.of("key1", "prefix1", "key2", "prefix2"));

        assertTrue(matcher.match(Map.of("key1", List.of("prefix1-foo"), "key2", List.of("prefix2-foo"))));
        assertFalse(matcher.match(Map.of("key1", List.of("prefix1-foo"), "key2", List.of("prefix1-foo"))));
    }

    @Test
    void matchAnyOfSameKey() {
        PrefixTokenMatcher matcher = new PrefixTokenMatcher(Map.of("key1", "prefix1"));

        assertTrue(matcher.match(Map.of("key1", List.of("prefix1-foo", "prefix-2"))));
    }

    @Test
    void matchFailAsSoonAsAnyOfRequiredClaimsIsNotMatching() {
        PrefixTokenMatcher matcher = new PrefixTokenMatcher(Map.of("key1", "prefix1", "key2", "prefix2"));

        assertFalse(matcher.match(Map.of("key1", List.of("prefix1-foo"))));
        assertFalse(matcher.match(Map.of("key1", List.of("prefix1-foo"), "key2", List.of("foobar"))));
    }
}
