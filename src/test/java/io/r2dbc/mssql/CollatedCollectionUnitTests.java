/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link CollatedCollection}.
 *
 * @author Mark Paluch
 */
class CollatedCollectionUnitTests {

    CollatedCollection collatedCollection = new CollatedCollection(Arrays.asList("one", "two", "three", "one"));

    @Test
    void setIsUnmodifiable() {

        assertThatThrownBy(() -> collatedCollection.add("foo")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> collatedCollection.addAll(Collections.emptyList())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> collatedCollection.remove("foo")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> collatedCollection.removeAll(Collections.emptyList())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> collatedCollection.retainAll(Collections.emptyList())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> collatedCollection.clear()).isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> collatedCollection.iterator().remove()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void containsConsidersNamingRules() {

        assertThat(collatedCollection.contains("one")).isTrue();
        assertThat(collatedCollection.contains("[one]")).isTrue();
        assertThat(collatedCollection.contains("[one")).isFalse();
        assertThat(collatedCollection.contains("one]")).isFalse();
        assertThat(collatedCollection.contains("[One]")).isFalse();
    }

    @Test
    void containsAllConsidersNamingRules() {

        assertThat(collatedCollection.containsAll(Collections.singleton("one"))).isTrue();
        assertThat(collatedCollection.containsAll(Collections.singleton("[one]"))).isTrue();
        assertThat(collatedCollection.containsAll(Collections.singleton("[one"))).isFalse();
        assertThat(collatedCollection.containsAll(Collections.singleton("one]"))).isFalse();
        assertThat(collatedCollection.containsAll(Collections.singleton("[One]"))).isFalse();
    }

    @Test
    void iteratesOverColumnsInOrder() {
        assertThat(collatedCollection.iterator()).toIterable().containsSequence("one", "two", "three", "one");
    }

    @Test
    void streamsOverColumnsInOrder() {
        assertThat(collatedCollection.stream().collect(Collectors.toList())).containsSequence("one", "two", "three", "one");
    }

    @Test
    void arrayContainsColumnsNames() {
        assertThat(collatedCollection.toArray(new String[0])).containsSequence("one", "two", "three");
    }
}
