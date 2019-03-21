/*
 * Copyright 2018-2019 the original author or authors.
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
 * Unit tests for {@link RowToken}.
 *
 * @author Mark Paluch
 */
class RowTokenUnitTests {

    @Test
    void shouldDecodeRow() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("8107000000000000" +
            "000800300B65006D0070006C006F0079" +
            "00650065005F00690064000000000008" +
            "00E764000904D00034096C0061007300" +
            "74005F006E0061006D00650000000000" +
            "0900A732000904D000340A6600690072" +
            "00730074005F006E0061006D00650000" +
            "00000009006E0806730061006C006100" +
            "7200790000000000090024100366006F" +
            "006F000000000009006D080366006C00" +
            "74000000000009006D04036200610072" +
            "00D1010C00700061006C007500630068" +
            "0004006D61726B080000000020A10700" +
            "10F17B0DC7C7E5C54098C7A12F7E6867" +
            "2408FED478E94628C6400437423146");

        Tabular tabular = Tabular.decode(buffer, true);

        assertThat(tabular.getTokens()).hasSize(2);

        RowToken rowToken = tabular.getRequiredToken(RowToken.class);
        assertThat(rowToken.getColumnData(0)).isNotNull();
        assertThat(rowToken.getColumnData(1)).isNotNull();
        assertThat(rowToken.getColumnData(2)).isNotNull();
        assertThat(rowToken.getColumnData(3)).isNotNull();
    }

    @Test
    void canDecodeShouldReportDecodability() {

        ByteBuf rowMetadata = HexUtils.decodeToByteBuf("8107000000000000" +
            "000800300B65006D0070006C006F0079" +
            "00650065005F00690064000000000008" +
            "00E764000904D00034096C0061007300" +
            "74005F006E0061006D00650000000000" +
            "0900A732000904D000340A6600690072" +
            "00730074005F006E0061006D00650000" +
            "00000009006E0806730061006C006100" +
            "7200790000000000090024100366006F" +
            "006F000000000009006D080366006C00" +
            "74000000000009006D04036200610072" +
            "00");

        ColumnMetadataToken columns = ColumnMetadataToken.decode(rowMetadata.skipBytes(1), true);

        String row = "010C00700061006C007500630068" +
            "0004006D61726B080000000020A10700" +
            "10F17B0DC7C7E5C54098C7A12F7E6867" +
            "2408FED478E94628C6400437423146";

        CanDecodeTestSupport.testCanDecode(HexUtils.decodeToByteBuf(row), buffer -> RowToken.canDecode(buffer, columns.getColumns()));
    }
}
