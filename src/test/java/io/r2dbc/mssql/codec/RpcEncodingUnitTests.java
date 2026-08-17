/*
 * Copyright 2020 the original author or authors.
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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RpcEncoding}.
 *
 * @author Mark Paluch
 */
class RpcEncodingUnitTests {

    @Test
    void shouldEncodeStringAsNVarchar() {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();
        RpcEncoding.encodeString(buffer, null, RpcDirection.IN, Collation.RAW, "hello world");

        byte[] unicode = "hello world".getBytes(ServerCharset.UNICODE.charset());

        String len = "16 00";
        EncodedAssert.assertThat(buffer).isEqualToHex("00 00 e7 40 1f 00 00 00 00 00 " + len + ByteBufUtil.hexDump(unicode));
    }

    @Test
    void shouldEncodeStringAsNVarcharMax() {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();
        RpcEncoding.encodeString(buffer, null, RpcDirection.OUT, Collation.RAW, "hello world");

        byte[] unicode = "hello world".getBytes(ServerCharset.UNICODE.charset());

        String uLongLen = "16 00 00 00 00 00 00 00";
        String len = "16 00 00 00";

        EncodedAssert.assertThat(buffer).isEqualToHex("00 01 e7 ff ff 00 00 00 00 00 " + uLongLen + len + ByteBufUtil.hexDump(unicode) + " 00 00 00 00");
    }
}
