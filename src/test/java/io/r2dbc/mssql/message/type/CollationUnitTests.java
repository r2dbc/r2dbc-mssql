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

package io.r2dbc.mssql.message.type;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Collation}.
 *
 * @author Mark Paluch
 */
class CollationUnitTests {

    @Test
    void shouldDecodeCp1252CollationFromSortId() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("0904D00034");

        Collation collation = Collation.decode(buffer);

        assertThat(collation.getSortId()).isEqualTo(52);
        assertThat(collation.getCharset()).isEqualTo(Charset.forName("windows-1252"));
    }

    @Test
    void shouldDecodeEnglishCollationFromSortId() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("0904D00000");

        Collation collation = Collation.decode(buffer);

        assertThat(collation.getSortId()).isEqualTo(0);
        assertThat(collation.getCharset()).isEqualTo(Charset.forName("windows-1252"));
    }
}
