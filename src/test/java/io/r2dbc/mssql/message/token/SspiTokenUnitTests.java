/*
 * Copyright 2026 the original author or authors.
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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SspiToken}.
 *
 * @author Daniel Pachali
 */
final class SspiTokenUnitTests {

    @Test
    void shouldDecodeSspiToken() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("ED040001020304");

        assertThat(buffer.readByte()).isEqualTo(SspiToken.TYPE);

        SspiToken token = SspiToken.decode(buffer);

        assertThat(token.getType()).isEqualTo(SspiToken.TYPE);
        assertThat(token.getName()).isEqualTo("SSPI");
        assertThat(token.getSspiBuffer()).containsExactly(new byte[]{0x01, 0x02, 0x03, 0x04});
        assertThat(buffer.isReadable()).isFalse();
    }

    @Test
    void canDecodeShouldReportDecodability() {

        CanDecodeTestSupport.testCanDecode(HexUtils.decodeToByteBuf("040001020304"), SspiToken::canDecode);
    }

    @Test
    void shouldDecodeEmptySspiBuffer() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("0000");

        SspiToken token = SspiToken.decode(buffer);

        assertThat(token.getSspiBuffer()).isEmpty();
        assertThat(buffer.isReadable()).isFalse();
    }

    @Test
    void shouldLeaveFollowingTokenDataUnread() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("0300010203AA");

        SspiToken token = SspiToken.decode(buffer);

        assertThat(token.getSspiBuffer()).containsExactly(new byte[]{0x01, 0x02, 0x03});
        assertThat(buffer.readableBytes()).isEqualTo(1);
        assertThat(buffer.readByte()).isEqualTo((byte) 0xAA);
    }

    @Test
    void shouldDecodeSspiTokenThroughTabular() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("ED040001020304");

        Tabular tabular = Tabular.decode(buffer, false);

        assertThat(tabular.getTokens()).hasSize(1);
        assertThat(tabular.getTokens().get(0)).isInstanceOf(SspiToken.class);

        SspiToken token = (SspiToken) tabular.getTokens().get(0);

        assertThat(token.getSspiBuffer()).containsExactly(new byte[]{0x01, 0x02, 0x03, 0x04});
    }

}
