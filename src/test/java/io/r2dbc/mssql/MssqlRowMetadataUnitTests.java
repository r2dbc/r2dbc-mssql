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

package io.r2dbc.mssql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MssqlRowMetadata}.
 *
 * @author Mark Paluch
 */
class MssqlRowMetadataUnitTests {

    TypeInformation integer = builder().withScale(4).withMaxLength(4).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withServerType(SqlServerType.INTEGER).build();

    Column column = new Column(0, "foo", integer, null);

    ByteBuf data = Unpooled.wrappedBuffer(new byte[]{(byte) 0x42, 0, 0, 0});

    RowToken rowToken = RowToken.decode(data, Collections.singletonList(column));

    MssqlRow row = new MssqlRow(new DefaultCodecs(), Collections.singletonList(column), Collections.singletonMap("foo", column), rowToken);

    @Test
    void shouldLookupMetadataByName() {

        MssqlRowMetadata metadata = new MssqlRowMetadata(row);
        MssqlColumnMetadata columnMetadata = metadata.getColumnMetadata("foo");

        assertThat(columnMetadata).isNotNull();
        assertThat(columnMetadata.getName()).isEqualTo("foo");
        assertThat(columnMetadata.getType()).isEqualTo(SqlServerType.INTEGER.ordinal());
        assertThat(columnMetadata.getTypeInformation()).isEqualTo(integer);
    }

    @Test
    void shouldLookupMetadataByIndex() {

        MssqlRowMetadata metadata = new MssqlRowMetadata(row);
        MssqlColumnMetadata columnMetadata = metadata.getColumnMetadata(0);

        assertThat(columnMetadata).isNotNull();
        assertThat(columnMetadata.getName()).isEqualTo("foo");
    }

    @Test
    void shouldReturnMetadataForAllCOlumns() {

        MssqlRowMetadata metadata = new MssqlRowMetadata(row);

        assertThat(metadata.getColumnMetadatas()).hasSize(1);
    }
}
