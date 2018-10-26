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

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Paluch
 */
class ErrorTokenUnitTests {

    @Test
    void shouldDecodeErrorToken() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("AA5C00CF00000001" +
            "101B0049006E00760061006C00690064" +
            "00200063006F006C0075006D006E0020" +
            "006E0061006D00650020002700660073" +
            "006400660027002E000C610036003800" +
            "38003000390032006100370039006600" +
            "35000001000000");

        assertThat(buffer.readByte()).isEqualTo(ErrorToken.TYPE);

        ErrorToken errorToken = ErrorToken.decode(buffer);

        assertThat(errorToken.getNumber()).isEqualTo(207);
        assertThat(errorToken.getState()).isEqualTo((byte) 1);
        assertThat(errorToken.getMessage()).isEqualTo("Invalid column name 'fsdf'.");
        assertThat(errorToken.getServerName()).isEqualTo("a688092a79f5");
        assertThat(errorToken.getClassification()).isEqualTo(AbstractInfoToken.Classification.GENERAL_ERROR);
        assertThat(errorToken.getProcName()).isEqualTo("");
        assertThat(errorToken.getLineNumber()).isEqualTo(16777216);
    }
}
