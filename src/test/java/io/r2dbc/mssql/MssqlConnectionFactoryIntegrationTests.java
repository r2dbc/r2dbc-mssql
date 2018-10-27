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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;

/**
 * @author Mark Paluch
 */
final class MssqlConnectionFactoryIntegrationTests {

    @Test
    @Disabled("Requires running SQL server")
    void shouldConnectToSqlServer() throws InterruptedException {

        Hooks.onOperatorDebug();
        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .host("localhost")
            .username("sa")
            .password("my1.password")
            .database("master")
            .build();

        MssqlConnectionFactory factory = new MssqlConnectionFactory(configuration);

        MssqlConnection connection = factory.create().block();

        Thread.sleep(100);
        connection.close().block();
    }
}
