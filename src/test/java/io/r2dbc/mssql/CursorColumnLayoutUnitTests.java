/*
 * Copyright 2026 the original author or authors.
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

import io.r2dbc.mssql.message.token.ColInfoToken;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.NbcRowToken;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.Types;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CursorColumnLayout}.
 *
 * @author dom54
 */
class CursorColumnLayoutUnitTests {

    static final TypeInformation FIXED_INT = TypeInformation.builder().withMaxLength(4).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withServerType(SqlServerType.INTEGER).build();

    // id (key column of table 1), name (table 1), ROWSTAT (hidden expression) as sent by SQL Server for a keyset cursor
    static final String CURSOR_COLINFO = "09 00 01 01 08 02 01 00 03 00 14";

    Column[] columns = {new Column(0, "id", Types.integer()), new Column(1, "name", Types.varchar(50)), new Column(2, "ROWSTAT", FIXED_INT)};

    @Test
    void shouldVerifyHiddenRowStatusColumn() {

        CursorColumnLayout layout = CursorColumnLayout.from(ColumnMetadataToken.create(this.columns), colInfo(CURSOR_COLINFO));

        assertThat(layout).isNotNull();
        assertThat(layout.getRowStatusIndex()).isEqualTo(2);
    }

    @Test
    void shouldDetectMissingRow() {

        CursorColumnLayout layout = CursorColumnLayout.from(ColumnMetadataToken.create(this.columns), colInfo(CURSOR_COLINFO));

        RowToken present = RowToken.decode(HexUtils.decodeToByteBuf("04 01 00 00 00 01 00 61 01 00 00 00"), this.columns);
        RowToken missing = RowToken.decode(HexUtils.decodeToByteBuf("04 01 00 00 00 FF FF 02 00 00 00"), this.columns);

        assertThat(layout.isMissing(present)).isFalse();
        assertThat(layout.isMissing(missing)).isTrue();

        present.release();
        missing.release();
    }

    @Test
    void shouldConsiderNullRowStatusAsPresent() {

        Column[] columns = {new Column(0, "id", Types.integer()), new Column(1, "ROWSTAT", Types.integer())};
        CursorColumnLayout layout = CursorColumnLayout.from(ColumnMetadataToken.create(columns), colInfo("06 00 01 01 08 02 00 14"));

        // null bitmap: ROWSTAT is null
        NbcRowToken row = NbcRowToken.decode(HexUtils.decodeToByteBuf("02 04 01 00 00 00"), columns);

        assertThat(layout.isMissing(row)).isFalse();

        row.release();
    }

    @Test
    void shouldNotVerifyUserColumnNamedRowstat() {

        // not hidden: SELECT id, name, CAST(2 AS int) AS ROWSTAT without row status column
        assertThat(CursorColumnLayout.from(ColumnMetadataToken.create(this.columns), colInfo("09 00 01 01 08 02 01 00 03 00 04"))).isNull();

        // hidden key column of a base table
        assertThat(CursorColumnLayout.from(ColumnMetadataToken.create(this.columns), colInfo("09 00 01 01 08 02 01 00 03 01 18"))).isNull();
    }

    @Test
    void shouldNotVerifyWithoutMatchingColumnInfo() {

        // COLINFO describes fewer columns
        assertThat(CursorColumnLayout.from(ColumnMetadataToken.create(this.columns), colInfo("06 00 01 01 08 02 00 14"))).isNull();

        // COLINFO column number does not match the trailing column
        assertThat(CursorColumnLayout.from(ColumnMetadataToken.create(this.columns), colInfo("09 00 01 01 08 02 01 00 02 00 14"))).isNull();
    }

    @Test
    void shouldNotVerifyNonIntegerOrDifferentlyNamedColumn() {

        Column[] varchar = {new Column(0, "id", Types.integer()), new Column(1, "name", Types.varchar(50)), new Column(2, "ROWSTAT", Types.varchar(50))};
        assertThat(CursorColumnLayout.from(ColumnMetadataToken.create(varchar), colInfo(CURSOR_COLINFO))).isNull();

        Column[] renamed = {new Column(0, "id", Types.integer()), new Column(1, "name", Types.varchar(50)), new Column(2, "status", FIXED_INT)};
        assertThat(CursorColumnLayout.from(ColumnMetadataToken.create(renamed), colInfo(CURSOR_COLINFO))).isNull();
    }

    private static ColInfoToken colInfo(String hex) {
        return ColInfoToken.decode(HexUtils.decodeToByteBuf(hex));
    }

}
