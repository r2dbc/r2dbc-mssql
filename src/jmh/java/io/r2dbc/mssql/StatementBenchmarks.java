/*
 * Copyright 2019 the original author or authors.
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for Statement execution modes. Contains the following execution methods:
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
public class StatementBenchmarks extends BenchmarkSettings {

    @State(Scope.Benchmark)
    public static class ConnectionHolder {

        final Connection jdbc;

        final MssqlConnection r2dbc;

        public ConnectionHolder() {

            try {

                MsSqlServerExtension extension = new MsSqlServerExtension();
                extension.initialize();

                jdbc = extension.getDataSource().getConnection();


                Statement statement = jdbc.createStatement();

                try {
                    statement.execute("DROP TABLE simple_test");
                } catch (SQLException e) {
                }

                statement.execute("CREATE TABLE simple_test (name VARCHAR(255))");
                statement.execute("INSERT INTO simple_test VALUES('foo')");
                statement.execute("INSERT INTO simple_test VALUES('bar')");
                statement.execute("INSERT INTO simple_test VALUES('baz')");


                MssqlConnectionConfiguration configuration =
                    extension.configBuilder().preferCursoredExecution(sql -> sql.contains(" /* cursored */")).build();
                r2dbc = new MssqlConnectionFactory(configuration).create().block();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Benchmark
    @Testable
    public void simpleDirectJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        Statement statement = connectionHolder.jdbc.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM simple_test");

        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("name"));
        }

        resultSet.close();
        statement.close();
    }

    @Benchmark
    @Testable
    public void simpleDirectR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) {

        MssqlStatement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM simple_test");
        statement.fetchSize(0);

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

    @Benchmark
    @Testable
    public void simpleCursoredJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        Statement statement = connectionHolder.jdbc.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM simple_test  /* cursored */");

        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("name"));
        }

        resultSet.close();
        statement.close();
    }

    @Benchmark
    public void simpleCursoredR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) {

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM simple_test /* cursored */");

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

    @Benchmark
    public void parametrizedDirectJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        PreparedStatement statement = connectionHolder.jdbc.prepareStatement("SELECT * FROM simple_test WHERE name = ?");
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

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM simple_test WHERE name = @P0").bind("P0", "foo");

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

    @Benchmark
    public void preparedCursoredJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        PreparedStatement statement = connectionHolder.jdbc.prepareStatement("SELECT * FROM simple_test WHERE name = ?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
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

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM simple_test WHERE name = @P0 /* cursored */").bind("P0", "foo");

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }
}
