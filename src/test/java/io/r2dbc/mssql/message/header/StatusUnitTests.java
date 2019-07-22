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

package io.r2dbc.mssql.message.header;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Status}.
 *
 * @author Mark Paluch
 */
class StatusUnitTests {

    @ParameterizedTest
    @EnumSource(Status.StatusBit.class)
    void shouldReturnCorrectStatus(Status.StatusBit bit) {

        if (bit == Status.StatusBit.NORMAL) {
            return;
        }

        Status status = Status.of(bit);

        assertThat(status.is(bit)).isTrue();
    }

    @Test
    void shouldCreateCombinedStatus() {

        Status status = Status.of(Status.StatusBit.EOM, Status.StatusBit.RESET_CONNECTION);

        assertThat(status.is(Status.StatusBit.EOM)).isTrue();
        assertThat(status.is(Status.StatusBit.RESET_CONNECTION)).isTrue();
        assertThat(status.is(Status.StatusBit.RESET_CONNECTION_SKIP_TRAN)).isFalse();
        assertThat(status.is(Status.StatusBit.IGNORE)).isFalse();
    }

    @Test
    void shouldAddStatus() {

        Status status = Status.of(Status.StatusBit.EOM).and(Status.StatusBit.RESET_CONNECTION);

        assertThat(status.is(Status.StatusBit.EOM)).isTrue();
        assertThat(status.is(Status.StatusBit.RESET_CONNECTION)).isTrue();

        assertThat(status.and(Status.StatusBit.EOM)).isSameAs(status);

        Status ignore = status.and(Status.StatusBit.IGNORE);
        assertThat(ignore.is(Status.StatusBit.IGNORE)).isTrue();
        assertThat(ignore).isNotSameAs(status);
    }

    @Test
    void shouldRemoveStatus() {

        Status status = Status.of(Status.StatusBit.EOM).and(Status.StatusBit.RESET_CONNECTION);

        assertThat(status.not(Status.StatusBit.IGNORE)).isSameAs(status);

        Status notEom = status.not(Status.StatusBit.EOM);
        assertThat(notEom.is(Status.StatusBit.EOM)).isFalse();
    }
}
