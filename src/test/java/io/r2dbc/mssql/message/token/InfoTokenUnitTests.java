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

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link InfoToken}.
 *
 * @author Mark Paluch
 */
final class InfoTokenUnitTests {

    @Test
    void shouldDecode() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("AB700045160000020025" + "004300680061006E0067006500640020"
            + "00640061007400610062006100730065" + "00200063006F006E0074006500780074" + "00200074006F00200027006D00610073"
            + "0074006500720027002E000C61003600" + "38003800300039003200610037003900" + "660035000001000000");

        assertThat(buffer.readByte()).isEqualTo(InfoToken.TYPE);

        InfoToken infoToken = InfoToken.decode(buffer);

        assertThat(infoToken.getNumber()).isEqualTo(5701);
        assertThat(infoToken.getState()).isEqualTo((byte) 2);
        assertThat(infoToken.getMessage()).isEqualTo("Changed database context to 'master'.");
        assertThat(infoToken.getServerName()).isEqualTo("a688092a79f5");
        assertThat(infoToken.getProcName()).isEqualTo("");
        assertThat(infoToken.getLineNumber()).isEqualTo(16777216);
    }

    @Test
    void shouldDecodeLanguageChange() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("ab74" +
            "0047160000010027004300680061006e" +
            "0067006500640020006c0061006e0067" +
            "00750061006700650020007300650074" +
            "00740069006e006700200074006f0020" +
            "00750073005f0065006e0067006c0069" +
            "00730068002e000c6100360038003800" +
            "30003900320061003700390066003500" +
            "0001000000");

        assertThat(buffer.readByte()).isEqualTo(InfoToken.TYPE);

        InfoToken infoToken = InfoToken.decode(buffer);

        assertThat(infoToken.getNumber()).isEqualTo(5703);
        assertThat(infoToken.getState()).isEqualTo((byte) 1);
        assertThat(infoToken.getMessage()).isEqualTo("Changed language setting to us_english.");
        assertThat(infoToken.getServerName()).isEqualTo("a688092a79f5");
        assertThat(infoToken.getProcName()).isEqualTo("");
        assertThat(infoToken.getLineNumber()).isEqualTo(16777216);
    }

    @Test
    void canDecodeShouldReportDecodability() {

        String data = "74" +
            "0047160000010027004300680061006e" +
            "0067006500640020006c0061006e0067" +
            "00750061006700650020007300650074" +
            "00740069006e006700200074006f0020" +
            "00750073005f0065006e0067006c0069" +
            "00730068002e000c6100360038003800" +
            "30003900320061003700390066003500" +
            "0001000000";

        CanDecodeTestSupport.testCanDecode(HexUtils.decodeToByteBuf(data), InfoToken::canDecode);
    }
}
