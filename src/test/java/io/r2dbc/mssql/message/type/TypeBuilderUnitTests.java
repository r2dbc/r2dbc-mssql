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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Paluch
 */
class TypeBuilderUnitTests {

    @Test
    void shouldDecodeInt() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("000000000800380B");
        TypeInformation typeInformation = TypeBuilder.decode(buffer, true);

        assertThat(typeInformation.getMaxLength()).isEqualTo(4);
        assertThat(typeInformation.getServerType()).isEqualTo(SqlServerType.INTEGER);
        assertThat(typeInformation.getLengthStrategy()).isEqualTo(LengthStrategy.FIXEDLENTYPE);
        assertThat(typeInformation.getDisplaySize()).isEqualTo(11);
    }

    @Test
    void canDecodeShouldCheckDecodingAbility() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("000000000800380B");

        assertThat(TypeBuilder.canDecode(buffer, true)).isTrue();
        assertThat(buffer.readerIndex()).isEqualTo(0);

        assertThat(TypeBuilder.canDecode(HexUtils.decodeToByteBuf("000000000800"), true)).isFalse();
    }
}
