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
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.Types;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NbcRowToken}.
 *
 * @author Mark Paluch
 */
class NbcRowTokenUnitTests {

    TypeInformation integerType = Types.integer();

    TypeInformation stringType = Types.varchar(255);

    List<Column> columns = Arrays.asList(new Column(0, "id", integerType),
        new Column(1, "first_name", stringType),
        new Column(2, "last_name", stringType),
        new Column(3, "other", stringType),
        new Column(4, "other2", stringType),
        new Column(5, "other3", stringType),
        new Column(6, "rowstat", integerType));

    @Test
    void shouldDecodeNbcRow() {

        ByteBuf data = HexUtils.decodeToByteBuf("D2 1C 04 01 00 00 00 01 00 61 02 00 78 61 04 01 00 00 00");

        assertThat(data.readByte()).isEqualTo(NbcRowToken.TYPE);

        NbcRowToken rowToken = NbcRowToken.decode(data, columns);

        assertThat(rowToken.getColumnData(0)).isNotNull();
        assertThat(rowToken.getColumnData(1)).isNotNull();
        assertThat(rowToken.getColumnData(2)).isNull();
        assertThat(rowToken.getColumnData(3)).isNull();
        assertThat(rowToken.getColumnData(4)).isNull();
        assertThat(rowToken.getColumnData(5)).isNotNull();
        assertThat(rowToken.getColumnData(6)).isNotNull();
    }

    @Test
    void canDecodeShouldReportDecodability() {

        String data = "1C 04 01 00 00 00 01 00 61 02 00 78 61 04 01 00 00 00";

        CanDecodeTestSupport.testCanDecode(HexUtils.decodeToByteBuf(data), buffer -> NbcRowToken.canDecode(buffer, columns));
    }
}
