/*
 * Copyright 2018-2020 the original author or authors.
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

package io.r2dbc.mssql.message.header;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Paluch
 */
final class HeaderUnitTests {

    @Test
    void shouldEncodePreLoginHeader() {

        Header header = new Header(Type.PRE_LOGIN, Status.of(Status.StatusBit.EOM), (short) 0x002F, (short) 0, (byte) 1, (byte) 0);

        ByteBuf buffer = Unpooled.buffer(8);

        header.encode(buffer);

        assertThat(buffer.writerIndex()).isEqualTo(8);
        assertThat(ByteBufUtil.prettyHexDump(buffer)).containsIgnoringCase("12 01 00 2F 00 00 01 00");
    }

    @Test
    void shouldDecodeLoginHeader() {

        Header header = new Header(Type.PRE_LOGIN, Status.of(Status.StatusBit.EOM), (short) 0x002F, (short) 0, (byte) 1, (byte) 0);

        ByteBuf buffer = Unpooled.buffer(8);

        header.encode(buffer);

        assertThat(Header.canDecode(buffer)).isTrue();

        Header decoded = Header.decode(buffer);

        assertThat(decoded.getType()).isEqualTo(Type.PRE_LOGIN);
        assertThat(decoded.getStatus().is(Status.StatusBit.EOM)).isTrue();
        assertThat(decoded.getLength()).isEqualTo((short) 0x002F);
        assertThat(decoded.getSpid()).isEqualTo((short) 0);
        assertThat(decoded.getPacketId()).isEqualTo((byte) 1);
        assertThat(decoded.getWindow()).isEqualTo((byte) 0);
    }

    @Test
    void shouldNotDecodeHeader() {

        ByteBuf buffer = Unpooled.buffer(7);

        assertThat(Header.canDecode(buffer)).isFalse();
    }
}
