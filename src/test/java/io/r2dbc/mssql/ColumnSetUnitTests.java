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
 * Unit tests for {@link ColumnSet}.
 *
 * @author Mark Paluch
 */
class ColumnSetUnitTests {

    ColumnSet columnSet = new ColumnSet(Arrays.asList("one", "two", "three", "one"));

    @Test
    void setIsUnmodifiable() {

        assertThatThrownBy(() -> columnSet.add("foo")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> columnSet.addAll(Collections.emptyList())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> columnSet.remove("foo")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> columnSet.removeAll(Collections.emptyList())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> columnSet.retainAll(Collections.emptyList())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> columnSet.clear()).isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> columnSet.iterator().remove()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void containsConsidersNamingRules() {

        assertThat(columnSet.contains("one")).isTrue();
        assertThat(columnSet.contains("[one]")).isTrue();
        assertThat(columnSet.contains("[one")).isFalse();
        assertThat(columnSet.contains("one]")).isFalse();
        assertThat(columnSet.contains("[One]")).isFalse();
    }

    @Test
    void containsAllConsidersNamingRules() {

        assertThat(columnSet.containsAll(Collections.singleton("one"))).isTrue();
        assertThat(columnSet.containsAll(Collections.singleton("[one]"))).isTrue();
        assertThat(columnSet.containsAll(Collections.singleton("[one"))).isFalse();
        assertThat(columnSet.containsAll(Collections.singleton("one]"))).isFalse();
        assertThat(columnSet.containsAll(Collections.singleton("[One]"))).isFalse();
    }

    @Test
    void iteratesOverColumnsInOrder() {
        assertThat(columnSet.iterator()).containsSequence("one", "two", "three", "one");
    }

    @Test
    void streamsOverColumnsInOrder() {
        assertThat(columnSet.stream().collect(Collectors.toList())).containsSequence("one", "two", "three", "one");
    }

    @Test
    void arrayContainsColumnsNames() {
        assertThat(columnSet.toArray(new String[0])).containsSequence("one", "two", "three");
    }
}
