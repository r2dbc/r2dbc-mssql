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

import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MssqlConnectionFactory}.
 * 
 * @author Mark Paluch
 */
final class MssqlConnectionFactoryUnitTests {

    @Test
    void constructorNoClientFactory() {
        assertThatNullPointerException().isThrownBy(() -> new MssqlConnectionFactory(null, MssqlConnectionConfiguration.builder()
            .host("test-host")
            .password("test-password")
            .username("test-username")
            .build()))
            .withMessage("clientFactory must not be null");
    }

    @Test
    void constructorNoConfiguration() {
        assertThatNullPointerException().isThrownBy(() -> new MssqlConnectionFactory(null))
            .withMessage("configuration must not be null");
    }

}
