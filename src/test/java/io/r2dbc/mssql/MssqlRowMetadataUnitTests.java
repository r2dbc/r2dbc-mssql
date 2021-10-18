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

package io.r2dbc.mssql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.spi.Nullability;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Unit tests for {@link MssqlRowMetadata}.
 *
 * @author Mark Paluch
 */
class MssqlRowMetadataUnitTests {

    Codecs codecs = new DefaultCodecs();

    TypeInformation integer = builder().withScale(5).withMaxLength(4).withPrecision(4).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withServerType(SqlServerType.INTEGER).build();

    Column column = new Column(0, "foo", this.integer, null);

    ByteBuf data = Unpooled.wrappedBuffer(new byte[]{(byte) 0x42, 0, 0, 0});

    MssqlRowMetadata rowMetadata = new MssqlRowMetadata(this.codecs, new Column[]{this.column}, Collections.singletonMap("foo", this.column));

    @Test
    void shouldLookupMetadataByName() {

        MssqlColumnMetadata columnMetadata = this.rowMetadata.getColumnMetadata("foo");

        assertThat(columnMetadata).isNotNull();
        assertThat(columnMetadata.getName()).isEqualTo("foo");
        assertThat(columnMetadata.getJavaType()).isEqualTo(Integer.class);
        assertThat(columnMetadata.getPrecision()).isEqualTo(4);
        assertThat(columnMetadata.getScale()).isEqualTo(5);
        assertThat(columnMetadata.getNullability()).isEqualTo(Nullability.NON_NULL);
        assertThat(columnMetadata.getNativeTypeMetadata()).isEqualTo(this.integer);
    }

    @Test
    void shouldRemoveRowstatIfLastColumn() {

        Column rowstat = new Column(0, "ROWSTAT", this.integer, null);

        MssqlRowMetadata rowMetadata1 = new MssqlRowMetadata(this.codecs, new Column[]{this.column, rowstat}, new HashMap<>());

        assertThat(rowMetadata1.getColumnNames()).containsOnly("foo");
        assertThat(rowMetadata1.getCount()).isOne();
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> rowMetadata1.getColumnMetadata("ROWSTAT"));
        assertThat(rowMetadata1.getColumnNames()).containsOnly("foo");

        MssqlRowMetadata rowMetadata2 = new MssqlRowMetadata(this.codecs, new Column[]{rowstat}, new HashMap<>());

        assertThat(rowMetadata2.getColumnNames()).isEmpty();
        assertThat(rowMetadata2.getCount()).isZero();
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> rowMetadata2.getColumnMetadata("ROWSTAT"));
        assertThat(rowMetadata2.getColumnNames()).isEmpty();
    }

    @Test
    void retainsRowstatIfNotLastColumn() {

        Column rowstat = new Column(0, "ROWSTAT", this.integer, null);
        Map<String, Column> nameKeyedColumns = new HashMap<>();
        nameKeyedColumns.put("foo", this.column);
        nameKeyedColumns.put("ROWSTAT", rowstat);

        MssqlRowMetadata rowMetadata = new MssqlRowMetadata(this.codecs, new Column[]{rowstat, this.column}, nameKeyedColumns);

        assertThat(rowMetadata.getColumnNames()).contains("foo", "ROWSTAT");
        assertThat(rowMetadata.getCount()).isEqualTo(2);
    }

    @Test
    void shouldLookupMetadataByIndex() {

        MssqlColumnMetadata columnMetadata = this.rowMetadata.getColumnMetadata(0);

        assertThat(columnMetadata).isNotNull();
        assertThat(columnMetadata.getName()).isEqualTo("foo");
    }

    @Test
    void shouldReturnMetadataForAllColumns() {
        assertThat(this.rowMetadata.getColumnMetadatas()).hasSize(1);
    }

    @Test
    @SuppressWarnings("cast")
    void shouldReturnOrderedColumnNames() {

        Column c1 = new Column(0, "one", this.integer, null);
        Column c2 = new Column(1, "two", this.integer, null);
        Column c3 = new Column(2, "three", this.integer, null);

        Map<String, Column> nameKeyedColumns = new HashMap<>();
        nameKeyedColumns.put(c1.getName(), c1);
        nameKeyedColumns.put(c2.getName(), c2);
        nameKeyedColumns.put(c3.getName(), c3);

        MssqlRowMetadata rowMetadata = new MssqlRowMetadata(this.codecs, new Column[]{c1, c2, c3}, nameKeyedColumns);

        assertThat(rowMetadata.getColumnNames()).hasSize(3).contains("one").doesNotContain("four");
        assertThat(rowMetadata.getColumnNames().contains("one")).isTrue();
        assertThat(rowMetadata.getColumnNames().contains("four")).isFalse();

        assertThat(rowMetadata.getColumnNames().contains("One")).isTrue();
        assertThat(rowMetadata.getColumnNames().contains("óne")).isFalse();
        assertThat(rowMetadata.getColumnNames().toArray()).contains("one", "two");
        assertThat(rowMetadata.getColumnNames().toArray(new String[0])).contains("one", "two");
    }

    @Test
    void shouldConsiderIdenticalMatchWhenEscaped() {

        Column c1 = new Column(0, "one", this.integer, null);
        Column c2 = new Column(1, "two", this.integer, null);
        Column c3 = new Column(1, "one", this.integer, null);

        Map<String, Column> nameKeyedColumns = new HashMap<>();
        nameKeyedColumns.put(c1.getName(), c1);
        nameKeyedColumns.put(c2.getName(), c2);
        nameKeyedColumns.put(c3.getName(), c3);

        MssqlRowMetadata rowMetadata = new MssqlRowMetadata(this.codecs, new Column[]{c1, c2, c3}, nameKeyedColumns);

        assertThat(rowMetadata.getColumnNames()).hasSize(3).isNotEmpty();
        assertThat(rowMetadata.getColumnNames().contains("one")).isTrue();
        assertThat(rowMetadata.getColumnNames().contains("[one]")).isTrue();
        assertThat(rowMetadata.getColumnNames().contains("[One]")).isFalse();
        assertThat(rowMetadata.getColumnNames().iterator()).toIterable().contains("one", "two", "one");
        assertThat(rowMetadata.getColumnNames().stream().collect(Collectors.toList())).containsSequence("one", "two", "one");
    }
}
