/*
 * Copyright 2018 the original author or authors.
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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.TDSVersion;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.ContextualTdsFragment;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import io.r2dbc.mssql.util.Version;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Login7}.
 *
 * @author Mark Paluch
 */
final class Login7UnitTests {

    @Test
    void shouldKeepSqlAuthenticationLoginPacketUnchanged() {

        Login7 login7 = Login7.builder()
            .serverName("localhost")
            .hostName("some-fancy-hostname")
            .username("sa")
            .password("super-secret")
            .database("master")
            .clientLibraryName("MyDriver")
            .applicationName("MyApp")
            .clientLibraryVersion(Version.parse("6.4"))
            .tdsVersion(TDSVersion.VER_DENALI).build();

        ByteBuf buffer = Unpooled.buffer(400);
        login7.encode(buffer);

        // Header: 100100eb00000100
        byte[] expected = ByteBufUtil.decodeHexDump("e300000004000074" + "401f0000000004060000000000000000"
            + "e003001800000000000000005e001300" + "8400020088000c00a0000500aa000900" + "bc000400c000080000000000d0000600"
            + "00000000000000000000000000000000" + "00000000000073006f006d0065002d00" + "660061006e00630079002d0068006f00"
            + "730074006e0061006d00650073006100" + "92a5f2a5a2a5f3a582a577a592a5f3a5" + "93a582a5f3a5e2a54d00790041007000"
            + "70006c006f00630061006c0068006f00" + "73007400dc0000004d00790044007200" + "69007600650072006d00610073007400"
            + "65007200040100000001ff");

        assertThat(ByteBufUtil.prettyHexDump(buffer))
            .isEqualTo(ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(expected)));
    }


    @Test
    void shouldEncodeIntegratedSecurityLoginPacket() {

        byte[] initialSspiToken = new byte[]{(byte) 0x60, (byte) 0x82, 0x01, 0x23};

        Login7 login7 = Login7.builder()
            .serverName("localhost")
            .hostName("some-fancy-hostname")
            .database("master")
            .clientLibraryName("MyDriver")
            .applicationName("MyApp")
            .clientLibraryVersion(Version.parse("6.4"))
            .tdsVersion(TDSVersion.VER_DENALI)
            .integratedSecurity(initialSspiToken)
            .build();

        ByteBuf buffer = Unpooled.buffer(400);
        login7.encode(buffer);

        assertThat(buffer.getIntLE(0)).isEqualTo(buffer.readableBytes());

        // OptionFlags2.fIntSecurity
        assertThat(buffer.getUnsignedByte(25) & 0x80).isEqualTo(0x80);

        // Username and password are omitted from integrated-security LOGIN7.
        assertThat(buffer.getUnsignedShortLE(42)).isZero();
        assertThat(buffer.getUnsignedShortLE(46)).isZero();

        int sspiOffset = buffer.getUnsignedShortLE(78);
        int sspiLength = buffer.getUnsignedShortLE(80);

        assertThat(sspiOffset).isGreaterThanOrEqualTo(94);
        assertThat(sspiLength).isEqualTo(initialSspiToken.length);
        assertThat(ByteBufUtil.getBytes(buffer, sspiOffset, sspiLength)).containsExactly(initialSspiToken);

        // cbSSPILong is not used for a short SSPI token.
        assertThat(buffer.getIntLE(90)).isZero();
    }

    @Test
    void shouldEncodeLongSspiLength() {

        byte[] initialSspiToken = new byte[0x10000];
        initialSspiToken[0] = 0x60;
        initialSspiToken[initialSspiToken.length - 1] = 0x01;

        Login7 login7 = Login7.builder()
            .serverName("localhost")
            .hostName("some-fancy-hostname")
            .database("master")
            .clientLibraryName("MyDriver")
            .applicationName("MyApp")
            .clientLibraryVersion(Version.parse("6.4"))
            .tdsVersion(TDSVersion.VER_DENALI)
            .integratedSecurity(initialSspiToken)
            .build();

        ByteBuf buffer = Unpooled.buffer(0x11000);
        login7.encode(buffer);

        assertThat(buffer.getUnsignedShortLE(80)).isEqualTo(0xFFFF);
        assertThat(buffer.getIntLE(90)).isEqualTo(initialSspiToken.length);
    }

    @Test
    void shouldEncodePacket() {

        Login7 login7 = Login7.builder()
            .serverName("localhost")
            .hostName("some-fancy-hostname")
            .username("sa")
            .password("super-secret")
            .database("master")
            .clientLibraryName("MyDriver")
            .applicationName("MyApp")
            .clientLibraryVersion(Version.parse("6.4"))
            .tdsVersion(TDSVersion.VER_DENALI).build();

        Mono.just(login7.encode(TestByteBufAllocator.TEST, 0))
            .cast(ContextualTdsFragment.class)
            .as(StepVerifier::create)
            .consumeNextWith(actual -> {
                assertThat(actual.getHeaderOptions().getType()).isEqualTo(Type.TDS7_LOGIN);
                assertThat(actual.getHeaderOptions().getStatus().getValue()).isEqualTo((byte) 0);
            }).verifyComplete();
    }
}
