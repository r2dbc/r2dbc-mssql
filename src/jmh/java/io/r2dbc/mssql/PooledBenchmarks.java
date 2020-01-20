/*
 * Copyright 2019-2020 the original author or authors.
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

import com.zaxxer.hikari.HikariDataSource;
import io.r2dbc.mssql.util.MsSqlServerExtension;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for direct Statement execution mode using connection pooling.
 *
 * @author Mark Paluch
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class PooledBenchmarks extends BenchmarkSettings {

    @State(Scope.Benchmark)
    public static class ConnectionHolder {

        final HikariDataSource jdbc;

        final ConnectionFactory r2dbc;

        public ConnectionHolder() {

            MsSqlServerExtension extension = new MsSqlServerExtension();
            extension.initialize();

            jdbc = extension.getDataSource();

            MssqlConnectionConfiguration configuration =
                MssqlConnectionConfiguration.builder().host(extension.getHost()).username(extension.getUsername()).password(extension.getPassword()).build();

            MssqlConnectionFactory mssqlConnectionFactory = new MssqlConnectionFactory(configuration);
            ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(mssqlConnectionFactory).maxSize(4).build();

            r2dbc = new ConnectionPool(poolConfiguration);
        }
    }

    @Benchmark
    @Testable
    public void simpleDirectJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        Connection connection = connectionHolder.jdbc.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM MSreplication_options");

        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("optname"));
        }

        resultSet.close();
        statement.close();

        connection.close();
    }

    @Benchmark
    public void simpleDirectR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) {

        String optname = Mono.usingWhen(connectionHolder.r2dbc.create(), connection -> {

                MssqlStatement statement = (MssqlStatement) connection.createStatement("SELECT * FROM MSreplication_options");
                statement.fetchSize(0);
                return Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("optname", String.class))).last();

            }, io.r2dbc.spi.Connection::close, (conn, err) -> conn.close(), io.r2dbc.spi.Connection::close
        ).block();

        voodoo.consume(optname);
    }
}
