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

import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.ClientMessageAssert;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RpcRequest}.
 *
 * @author Mark Paluch
 */
class RpcRequestUnitTests {

    @Test
    void shouldEncodeSpCursorOpen() {

        int SCROLLOPT_FAST_FORWARD = 16;

        int CCOPT_READ_ONLY = 1;
        int CCOPT_ALLOW_DIRECT = 8192;

        int resultSetScrollOpt = SCROLLOPT_FAST_FORWARD;
        int resultSetCCOpt = CCOPT_READ_ONLY | CCOPT_ALLOW_DIRECT;

        Collation collation = Collation.from(13632521, 52);

        RpcRequest rpcRequest = RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorOpen) //
            .withTransactionDescriptor(TransactionDescriptor.empty())
            .withParameter(RpcDirection.OUT, 0) // cursor
            .withParameter(RpcDirection.IN, collation, "SELECT * FROM my_table")
            .withParameter(RpcDirection.IN, resultSetScrollOpt)  // scrollopt
            .withParameter(RpcDirection.IN, resultSetCCOpt) // ccopt
            .withParameter(RpcDirection.OUT, 0) // rowcount
            .build();

        String hex = "00 01 26 04 04 00 00 00 00 00 00 E7" +
            "40 1F 00 D0 04 09 34 2C 00 53 00 45 00 4C 00 45" +
            "00 43 00 54 00 20 00 2A 00 20 00 46 00 52 00 4F" +
            "00 4D 00 20 00 6D 00 79 00 5F 00 74 00 61 00 62" +
            "00 6C 00 65 00 00 00 26 04 04 10 00 00 00 00 00" +
            "26 04 04 01 20 00 00 00 01 26 04 04 00 00 00 00";

        ClientMessageAssert.assertThat(rpcRequest).encoded()
            .hasHeader(HeaderOptions.create(Type.RPC, Status.empty()))
            .isEncodedAs(expected -> {

                AllHeaders.transactional(TransactionDescriptor.empty(), 1).encode(expected);

                Encode.uShort(expected, 0xFFFF); // proc Id switch
                Encode.uShort(expected, 0x02); // proc Id
                Encode.asByte(expected, 0); // option flag
                Encode.asByte(expected, 0); // status flag

                expected.writeBytes(HexUtils.decodeToByteBuf(hex)); // encoded parameters
            });
    }
}
