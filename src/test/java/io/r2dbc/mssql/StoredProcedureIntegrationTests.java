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

import com.microsoft.sqlserver.jdbc.Geography;
import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.Parameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for stored procedure calls.
 */
class StoredProcedureIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void setUp() {

        dropProcedure("codec_spatial_in");
        dropProcedure("codec_spatial_out");
        dropProcedure("codec_spatial_null_out");

        SERVER.getJdbcOperations().execute("CREATE PROCEDURE codec_spatial_in\n" +
                "    @Geography geography,\n" +
                "    @Geometry geometry\n" +
                "AS\n" +
                "    SET NOCOUNT ON;\n" +
                "    SELECT @Geography AS Geography, @Geometry AS Geometry;");

        SERVER.getJdbcOperations().execute("CREATE PROCEDURE codec_spatial_out\n" +
                "    @Geography geography OUTPUT,\n" +
                "    @Geometry geometry OUTPUT\n" +
                "AS\n" +
                "    SET NOCOUNT ON;\n" +
                "    SET @Geography = geography::STGeomFromText('POINT(-122.35 37.55)', 4326);\n" +
                "    SET @Geometry = geometry::STGeomFromText('POINT(30 10)', 0);");

        SERVER.getJdbcOperations().execute("CREATE PROCEDURE codec_spatial_null_out\n" +
                "    @Geography geography OUTPUT,\n" +
                "    @Geometry geometry OUTPUT\n" +
                "AS\n" +
                "    SET NOCOUNT ON;\n" +
                "    SET @Geography = NULL;\n" +
                "    SET @Geometry = NULL;");
    }

    @AfterEach
    void tearDown() {

        dropProcedure("codec_spatial_in");
        dropProcedure("codec_spatial_out");
        dropProcedure("codec_spatial_null_out");
    }

    @Test
    void shouldPassNullSpatialParameters() {

        connection.createStatement("EXEC codec_spatial_in @Geography, @Geometry")
                .bind("@Geography", Parameters.in(SqlServerType.GEOGRAPHY))
                .bind("@Geometry", Parameters.in(SqlServerType.GEOMETRY))
                .execute()
                .flatMap(it -> it.map(readable -> new Object[]{readable.get(0, Geography.class), readable.get(1, Geometry.class)}))
                .as(StepVerifier::create)
                .consumeNextWith(actual -> {
                    assertThat(actual[0]).isNull();
                    assertThat(actual[1]).isNull();
                })
                .verifyComplete();
    }

    @Test
    void shouldDecodeNullSpatialOutParameters() {

        connection.createStatement("EXEC codec_spatial_null_out @Geography OUTPUT, @Geometry OUTPUT")
                .bind("@Geography", Parameters.out(SqlServerType.GEOGRAPHY))
                .bind("@Geometry", Parameters.out(SqlServerType.GEOMETRY))
                .execute()
                .flatMap(it -> it.map(readable -> new Object[]{readable.get(0, Geography.class), readable.get(1, Geometry.class)}))
                .as(StepVerifier::create)
                .consumeNextWith(actual -> {
                    assertThat(actual[0]).isNull();
                    assertThat(actual[1]).isNull();
                })
                .verifyComplete();
    }

    @Test
    void shouldDecodeSpatialOutParameters() throws SQLServerException {

        Geography geography = Geography.STGeomFromText("POINT(-122.35 37.55)", 4326);
        Geometry geometry = Geometry.STGeomFromText("POINT(30 10)", 0);

        connection.createStatement("EXEC codec_spatial_out @Geography OUTPUT, @Geometry OUTPUT")
                .bind("@Geography", Parameters.out(SqlServerType.GEOGRAPHY))
                .bind("@Geometry", Parameters.out(SqlServerType.GEOMETRY))
                .execute()
                .flatMap(it -> it.map(readable -> new Object[]{readable.get(0, Geography.class), readable.get(1, Geometry.class)}))
                .as(StepVerifier::create)
                .consumeNextWith(actual -> {
                    assertThat(((Geography) actual[0]).serialize()).isEqualTo(geography.serialize());
                    assertThat(((Geometry) actual[1]).serialize()).isEqualTo(geometry.serialize());
                })
                .verifyComplete();
    }

    private static void dropProcedure(String procedureName) {

        try {
            SERVER.getJdbcOperations().execute("DROP PROCEDURE " + procedureName);
        } catch (DataAccessException ignore) {
        }
    }

}
