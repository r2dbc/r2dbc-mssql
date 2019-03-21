/*
 * Copyright 2018 the original author or authors.
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
 * Unit tests for {@link ColumnMetadataToken}.
 *
 * @author Mark Paluch
 */
class ColumnMetadataTokenUnitTests {

    @Test
    void shouldDecodeColumns() {

        String encoded = "04000000000000" +
            "000800380B65006D0070006C006F0079" +
            "00650065005F00690064000000000008" +
            "00A732000904D00034096C0061007300" +
            "74005F006E0061006D00650000000000" +
            "0900A732000904D000340A6600690072" +
            "00730074005F006E0061006D00650000" +
            "00000009006E0806730061006C006100" +
            "72007900D1010000000600";

        ByteBuf buffer = HexUtils.decodeToByteBuf(encoded);

        ColumnMetadataToken metadata = ColumnMetadataToken.decode(buffer, true);

        assertThat(metadata.getColumns()).hasSize(4).extracting(Column::getName).containsSequence("employee_id", "last_name", "first_name", "salary");
    }
}
