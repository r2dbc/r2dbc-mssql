/*
 * Copyright 2018-2022 the original author or authors.
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
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Types;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link MssqlRow}.
 *
 * @author Mark Paluch
 */
class MssqlRowUnitTests {

    Codecs codecs = new DefaultCodecs();

    TypeInformation integer = Types.integer();

    Column column = new Column(0, "foo", this.integer, null);

    ByteBuf data = Unpooled.wrappedBuffer(new byte[]{(byte) 0x4, 0x42, 0, 0, 0});

    RowToken rowToken = RowToken.decode(this.data, new Column[]{this.column});

    MssqlRowMetadata rowMetadata = new MssqlRowMetadata(this.codecs, new Column[]{this.column}, Collections.singletonMap("foo", this.column));

    MssqlRow row = new MssqlRow(this.codecs, this.rowToken, this.rowMetadata);

    @Test
    void shouldReadRowByIndex() {
        assertThat(this.row.get(0)).isEqualTo(66);
        assertThat(this.row.get(0, Short.class)).isEqualTo((short) 66);
        assertThat(this.row.get(0, Integer.class)).isEqualTo(66);
        assertThat(this.row.get(0, Long.class)).isEqualTo(66L);
    }

    @Test
    void shouldReadRowByName() {
        assertThat(this.row.get("foo")).isEqualTo(66);
        assertThat(this.row.get("foo", Integer.class)).isEqualTo(66);
    }

    @Test
    void releaseShouldDeallocateResources() {

        this.row.release();
        assertThatThrownBy(() -> this.row.get("foo")).isInstanceOf(IllegalStateException.class).hasMessage("Value cannot be retrieved after row has been released");
    }
}
