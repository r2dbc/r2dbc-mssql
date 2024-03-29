/*
 * Copyright 2019-2022 the original author or authors.
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

import io.r2dbc.mssql.util.MsSqlServerExtension;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for Statement execution modes across various result sizes. Contains the following execution methods:
 *
 * <ul>
 * <li>SQLBATCH (Direct, Statements without parameters)</li>
 * <li>SP_EXECUTESQL (Direct, Statements with parameters)</li>
 * <li>SP_CURSOROPEN (Cursors, Statements without parameters)</li>
 * <li>SP_CURSORPREPEXEC (Cursors, Prepared statements)</li>
 * </ul>
 *
 * @author Mark Paluch
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class StagedResultSizeBenchmarks extends BenchmarkSettings {

    @State(Scope.Benchmark)
    public static class ConnectionHolder {

        final Connection jdbc;

        final io.r2dbc.spi.Connection r2dbc;

        @Param({"1", "10", "100", "200"})
        int resultSize;

        public ConnectionHolder() {

            try {
                MsSqlServerExtension extension = new MsSqlServerExtension();
                extension.initialize();
                this.jdbc = extension.getDataSource().getConnection();


                MssqlConnectionConfiguration configuration =
                    extension.configBuilder().preferCursoredExecution(sql -> sql.contains(" /* cursored */")).build();
                this.r2dbc = new MssqlConnectionFactory(configuration).create().block();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Setup
        public void setup() {
            try {
                Statement statement = this.jdbc.createStatement();

                try {
                    statement.execute("DROP TABLE result_sizes");
                } catch (SQLException e) {
                }

                statement.execute("CREATE TABLE result_sizes (id int, name VARCHAR(255))");

                for (int i = 0; i < this.resultSize; i++) {
                    statement.execute(String.format("INSERT INTO result_sizes VALUES(%d, '%s')", i, UUID.randomUUID()));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Benchmark
    @Testable
    public void simpleDirectJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        Statement statement = connectionHolder.jdbc.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM result_sizes");

        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("name"));
        }

        resultSet.close();
        statement.close();
    }

    @Benchmark
    @Testable
    public void simpleDirectR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) {

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM result_sizes");
        ((MssqlStatement) statement).fetchSize(0);

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

    @Benchmark

    public void simpleCursoredJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        Statement statement = connectionHolder.jdbc.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(128);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM result_sizes  /* cursored */");

        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("name"));
        }

        resultSet.close();
        statement.close();
    }

    @Benchmark
    public void simpleCursoredR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) {

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM result_sizes /* cursored */");

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

    @Benchmark
    public void parametrizedDirectJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        PreparedStatement statement = connectionHolder.jdbc.prepareStatement("SELECT * FROM result_sizes WHERE name != ?");
        statement.setString(1, "foo");

        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("name"));
        }

        resultSet.close();
        statement.close();
    }

    @Benchmark
    public void parametrizedDirectR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM result_sizes WHERE name != @P0").bind("P0", "foo");
        ((MssqlStatement) statement).fetchSize(0);

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

    @Benchmark
    public void preparedCursoredJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        PreparedStatement statement = connectionHolder.jdbc.prepareStatement("SELECT * FROM result_sizes WHERE name != ?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(128);
        statement.setString(1, "foo");

        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("name"));
        }

        resultSet.close();
        statement.close();
    }

    @Benchmark
    public void preparedCursoredR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) {

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM result_sizes WHERE name != @P0 /* cursored */").bind("P0", "foo");

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }
}
