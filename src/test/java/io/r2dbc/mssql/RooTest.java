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

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.logging.LogManager;
import java.util.stream.IntStream;

/**
 *
 */
public class RooTest {

    static {

        try {
            LogManager.getLogManager().readConfiguration(RooTest.class.getClassLoader().getResourceAsStream("logging.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void jdbc() throws Exception {

        new SQLServerDriver();

        Instant parse = Instant.parse("2018-08-27T17:41:14.890Z");
        Timestamp from = Timestamp.from(parse);

        Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;database=master", "sa", "A_Str0ng_Required_Password");

        try (PreparedStatement s = connection.prepareStatement("DECLARE @t TABLE(i INT);\n"
                + "INSERT INTO @t VALUES (?),(2),(3);\n"
                + "SELECT * FROM @t;\n", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

            s.setInt(1, 1);
            s.setFetchSize(1);

            boolean b = s.execute();
            while (true) {
                int c = -1;

                if (b) {
                    try (ResultSet rs = s.getResultSet()) {
                        while (rs.next())
                            System.out.println("rs: " + rs.getInt(1));
                    }
                } else {
                    c = s.getUpdateCount();
                }

                System.out.println(b + ": " + c);

                if (!b && c == -1) {
                    break;
                } else {
                    b = s.getMoreResults();
                }
            }
        }

        connection.createStatement().execute("DELETE FROM codec_test");
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO codec_test values(?)");
        preparedStatement.setObject(1, from);

        preparedStatement.execute();

        ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM codec_test");
        resultSet.next();

        for (int i = 0; i < 4; i++) {
            Object object = resultSet.getObject(i + 1);
            System.out.println(object + " : " + object.getClass().getName());
        }

    }

    private String getQuery() {
        StringBuffer stringBuffer = new StringBuffer("select ");
        IntStream.range(1, 4000).
            forEach(value -> stringBuffer.append("sysdatetime(),"));
        stringBuffer.append("sysdatetime() A, @P0");
        System.out.println("Length of String: " + stringBuffer.toString().length());
        return stringBuffer.toString();
    }

    @Test
    void r2dbc() {
        io.r2dbc.spi.Connection connection = Mono.from(ConnectionFactories.get("r2dbc:sqlserver://sa:A_Str0ng_Required_Password@localhost/master").create()).block();

        /*
        Flux.from(connection.createBatch().add("DECLARE @t TABLE(i INT)").add(
                "INSERT INTO @t VALUES (1),(2),(3)").add("SELECT * FROM @t;\n").execute())
            .flatMap(result -> {

                System.out.println("Result " + result);

                return result.getRowsUpdated();
            }).blockLast();
*/

        Flux.from(connection.createStatement("DECLARE @t TABLE(i INT);INSERT INTO @t VALUES (@P1),(2),(3);SELECT * FROM @t;\n").fetchSize(1).bind("@P1", 1)
            .execute()).flatMap(it -> it.getRowsUpdated()).doOnNext(it -> System.out.println(it)).blockLast();
    }

}
