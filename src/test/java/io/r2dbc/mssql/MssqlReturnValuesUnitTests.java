/*
 * Copyright 2021 the original author or authors.
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

package io.r2dbc.mssql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Types;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MssqlReturnValues}.
 *
 * @author Mark Paluch
 */
class MssqlReturnValuesUnitTests {

    TypeInformation integer = Types.integer();

    Column column = new Column(0, "foo", this.integer, null);

    ByteBuf data1 = Unpooled.wrappedBuffer(new byte[]{(byte) 0x4, 0x42, 0, 0, 0});

    ByteBuf data2 = Unpooled.wrappedBuffer(new byte[]{(byte) 0x4, 0x43, 0, 0, 0});

    ReturnValue first = new ReturnValue(6, "@First", 0, this.integer, this.data1);

    ReturnValue second = new ReturnValue(17, "@Second", 0, this.integer, this.data2);

    MssqlReturnValues returnValues = MssqlReturnValues.toReturnValues(new DefaultCodecs(), Arrays.asList(this.first, this.second));

    @Test
    void metadataShouldReportColumnNames() {

        MssqlReturnValuesMetadata metadata = this.returnValues.getMetadata();
        assertThat(metadata).containsExactly("@First", "@Second");
        assertThat(metadata.getColumnNames()).containsExactly("@First", "@Second");
    }

    @Test
    void metadataShouldReportCorrectType() {

        MssqlReturnValuesMetadata metadata = this.returnValues.getMetadata();
        assertThat(metadata.getColumnMetadata(0).getName()).isEqualTo("@First");
        assertThat(metadata.getColumnMetadata(0).getType()).isEqualTo(SqlServerType.INTEGER);
        assertThat(metadata.getColumnMetadata("first").getName()).isEqualTo("@First");
        assertThat(metadata.getColumnMetadata("@FIRST").getName()).isEqualTo("@First");

        assertThat(metadata.getColumnMetadata(1).getName()).isEqualTo("@Second");
        assertThat(metadata.getColumnMetadata(1).getType()).isEqualTo(SqlServerType.INTEGER);
    }

    @Test
    void getShouldReturnValue() {

        assertThat(this.returnValues.get(0)).isEqualTo(0x42);
        assertThat(this.returnValues.get(0)).isEqualTo(0x42);
        assertThat(this.returnValues.get("First")).isEqualTo(0x42);
        assertThat(this.returnValues.get("@First")).isEqualTo(0x42);

        assertThat(this.returnValues.get(1)).isEqualTo(0x43);
        assertThat(this.returnValues.get(1)).isEqualTo(0x43);
        assertThat(this.returnValues.get("Second")).isEqualTo(0x43);
        assertThat(this.returnValues.get("@Second")).isEqualTo(0x43);
    }

}
