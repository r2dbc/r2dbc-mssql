/*
 * Copyright 2018-2019 the original author or authors.
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

package io.r2dbc.mssql.message.token;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link Identifier}.
 *
 * @author Mark Paluch
 */
class IdentifierUnitTests {

    @Test
    void shouldCreateObjectIdentifier() {

        Identifier identifier = Identifier.objectName("foo");

        assertThat(identifier.asEscapedString()).isEqualTo("[foo]");
    }

    @Test
    void shouldCreateFullQualifier() {

        Identifier identifier = Identifier.builder().objectName("obj").schemaName("schema").databaseName("db").serverName("server").build();

        assertThat(identifier.asEscapedString()).isEqualTo("[server].[db].[schema].[obj]");
    }

    @Test
    void shouldRejectServernameWithoutDatabase() {

        assertThatThrownBy(() -> Identifier.builder().objectName("obj").serverName("server").build()).isInstanceOf(IllegalStateException.class);
    }
}
