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

package io.r2dbc.mssql.message.token;

import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.Encode;
import org.junit.jupiter.api.Test;

import static io.r2dbc.mssql.util.ClientMessageAssert.assertThat;

/**
 * Unit tests for {@link SqlBatch}.
 *
 * @author Mark Paluch
 */
class SqlBatchUnitTests {

    @Test
    void shouldEncodeProperly() {

        SqlBatch batch = SqlBatch.create(1, TransactionDescriptor.empty(), "SELECT *  FROM employees;");

        assertThat(batch).encoded() //
            .hasHeader(HeaderOptions.create(Type.SQL_BATCH, Status.empty())) //
            .isEncodedAs(it -> {

                Encode.dword(it, 22); // Total header length
                Encode.dword(it, 18); // MARS header length
                Encode.uShort(it, 2); // Tx Descriptor header
                it.writeBytes(new byte[8]); // Tx descriptor
                Encode.dword(it, 1); // outstanding requests

                Encode.unicodeStream(it, batch.getSql());
            });
    }
}
