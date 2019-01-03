/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql.message.token;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Paluch
 */
final class TokenIdentifierUnitTests {

    @Test
    void shouldParseZeroLengthToken() {

        TokenIdentifier identifier = create("00001000");

        assertThat(identifier.isZeroLength()).isTrue();
        assertThat(identifier.isFixedLength()).isFalse();
        assertThat(identifier.isFixedLength()).isFalse();
    }

    @Test
    void shouldParseFixedLengthToken() {

        TokenIdentifier identifier = create("00001100");

        assertThat(identifier.isZeroLength()).isFalse();
        assertThat(identifier.isFixedLength()).isTrue();
        assertThat(identifier.isVariableLength()).isFalse();

        assertThat(create("00001100").getFixedLength()).isEqualTo(1);
        assertThat(create("00011100").getFixedLength()).isEqualTo(2);
        assertThat(create("00101100").getFixedLength()).isEqualTo(4);
        assertThat(create("00111100").getFixedLength()).isEqualTo(8);
        assertThat(create("10111101").getFixedLength()).isEqualTo(8);
    }

    @Test
    void shouldParseVariableLength() {

        TokenIdentifier identifier = create("00000000");

        assertThat(identifier.isZeroLength()).isFalse();
        assertThat(identifier.isFixedLength()).isFalse();
        assertThat(identifier.isVariableLength()).isTrue();
    }

    private static TokenIdentifier create(String bitmask) {
        return TokenIdentifier.of(Integer.parseInt(bitmask, 2));
    }
}
