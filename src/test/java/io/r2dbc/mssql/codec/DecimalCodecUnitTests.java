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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DecimalCodec}.
 *
 * @author Mark Paluch
 */
class DecimalCodecUnitTests {

    @Test
    void shouldDecodeNumeric5x2() {

        TypeInformation type = TypeInformation.builder().withLengthStrategy(TypeInformation.LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.NUMERIC).withScale(2).withPrecision(5).build();

        Column column = ColumnUtil.createColumn(type);

        ByteBuf buffer = HexUtils.decodeToByteBuf("0501690E0000");

        BigDecimal decoded = DecimalCodec.INSTANCE.decode(buffer, column, BigDecimal.class);

        assertThat(decoded).isEqualTo("36.89");
    }
}
