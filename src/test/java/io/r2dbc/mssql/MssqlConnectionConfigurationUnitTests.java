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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

/**
 * Unit tests for {@link  MssqlConnectionConfiguration}.
 * 
 * @author Mark Paluch
 */
final class MssqlConnectionConfigurationUnitTests {

    @Test
    void builderNoApplicationName() {
        assertThatNullPointerException().isThrownBy(() -> MssqlConnectionConfiguration.builder().appName(null))
            .withMessage("appName must not be null");
    }
    
    @Test
    void builderNoConnectionId() {
        assertThatNullPointerException().isThrownBy(() -> MssqlConnectionConfiguration.builder().connectionId(null))
            .withMessage("connectionId must not be null");
    }

    @Test
    void builderNoHost() {
        assertThatNullPointerException().isThrownBy(() -> MssqlConnectionConfiguration.builder().host(null))
            .withMessage("host must not be null");
    }

    @Test
    void builderNoPassword() {
        assertThatNullPointerException().isThrownBy(() -> MssqlConnectionConfiguration.builder().password(null))
            .withMessage("password must not be null");
    }

    @Test
    void builderNoUsername() {
        assertThatNullPointerException().isThrownBy(() -> MssqlConnectionConfiguration.builder().username(null))
            .withMessage("username must not be null");
    }

    @Test
    void configuration() {
        UUID connectionId = UUID.randomUUID();
        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .connectionId(connectionId)
            .database("test-database")
            .host("test-host")
            .password("test-password")
            .port(100)
            .username("test-username")
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("connectionId", connectionId)
            .hasFieldOrPropertyWithValue("database", Optional.of("test-database"))
            .hasFieldOrPropertyWithValue("host", "test-host")
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("port", 100)
            .hasFieldOrPropertyWithValue("username", "test-username");
    }

    @Test
    void configurationDefaults() {
        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .appName("r2dbc")
            .database("test-database")
            .host("test-host")
            .password("test-password")
            .username("test-username")
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("appName", "r2dbc")
            .hasFieldOrPropertyWithValue("database", Optional.of("test-database"))
            .hasFieldOrPropertyWithValue("host", "test-host")
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("port", 1433)
            .hasFieldOrPropertyWithValue("username", "test-username");
    }

    @Test
    void constructorNoNoHost() {
        assertThatNullPointerException().isThrownBy(() -> MssqlConnectionConfiguration.builder()
            .password("test-password")
            .username("test-username")
            .build())
            .withMessage("host must not be null");
    }

    @Test
    void constructorNoPassword() {
        assertThatNullPointerException().isThrownBy(() -> MssqlConnectionConfiguration.builder()
            .host("test-host")
            .username("test-username")
            .build())
            .withMessage("password must not be null");
    }

    @Test
    void constructorNoUsername() {
        assertThatNullPointerException().isThrownBy(() -> MssqlConnectionConfiguration.builder()
            .host("test-host")
            .password("test-password")
            .build())
            .withMessage("username must not be null");
    }
}
