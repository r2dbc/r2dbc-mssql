/*
 * Copyright 2018-2021 the original author or authors.
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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ColInfoToken}.
 *
 * @author Mark Paluch
 */
class ColInfoTokenUnitTests {

    @Test
    void shouldDecodeToken() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("a50900010100020100030014");

        assertThat(buffer.readByte()).isEqualTo(ColInfoToken.TYPE);

        ColInfoToken token = ColInfoToken.decode(buffer);

        assertThat(token.getColumns()).hasSize(3);

        ColInfoToken.ColInfo column = token.getColumns().get(0);

        assertThat(column.getTable()).isEqualTo((byte) 1);
        assertThat(column.getColumn()).isEqualTo((byte) 1);
        assertThat(column.getStatus()).isEqualTo((byte) 0);
        assertThat(column.getName()).isNull();
        assertThat(buffer.readableBytes()).isZero();
    }

    @Test
    void shouldSkipToken() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("a50900010100020100030014");

        assertThat(buffer.readByte()).isEqualTo(ColInfoToken.TYPE);

        ColInfoToken token = ColInfoToken.skip(buffer);

        assertThat(token.getColumns()).isEmpty();
        assertThat(buffer.readableBytes()).isZero();
    }

    @Test
    void canDecodeShouldReportDecodability() {

        String data = "0900010100020100030014";

        CanDecodeTestSupport.testCanDecode(HexUtils.decodeToByteBuf(data), ColInfoToken::canDecode);
    }
}
