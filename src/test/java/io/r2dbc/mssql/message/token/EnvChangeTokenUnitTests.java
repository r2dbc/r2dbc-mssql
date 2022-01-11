/*
 * Copyright 2018-2022 the original author or authors.
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
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.tds.Redirect;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link EnvChangeToken}.
 *
 * @author Mark Paluch
 */
final class EnvChangeTokenUnitTests {

    @Test
    void shouldDecodeDatabaseChange() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("E31B0001066D0061007300740065007200066D0061007300740065007200");

        assertThat(buffer.readByte()).isEqualTo(EnvChangeToken.TYPE);

        EnvChangeToken token = EnvChangeToken.decode(buffer);

        assertThat(token.getChangeType()).isEqualTo(EnvChangeToken.EnvChangeType.Database);
        assertThat(token.getNewValueString()).isEqualTo("master");
        assertThat(token.getOldValueString()).isEqualTo("master");
    }

    @Test
    void shouldDecodeLanguageChange() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("e31700020a750073005f0065" + "006e0067006c0069007300680000");

        assertThat(buffer.readByte()).isEqualTo(EnvChangeToken.TYPE);

        EnvChangeToken token = EnvChangeToken.decode(buffer);

        assertThat(token.getChangeType()).isEqualTo(EnvChangeToken.EnvChangeType.Language);
        assertThat(token.getNewValueString()).isEqualTo("us_english");
        assertThat(token.getOldValueString()).isEqualTo("");
    }

    @Test
    void canDecodeShouldReportDecodability() {

        String data = "1B0001066D0061007300740065007200066D0061007300740065007200";

        CanDecodeTestSupport.testCanDecode(HexUtils.decodeToByteBuf(data), EnvChangeToken::canDecode);
    }

    @Test
    void shouldDecodeRoute() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("e316001413000039300700740065007300740069006e006700");

        assertThat(buffer.readByte()).isEqualTo(EnvChangeToken.TYPE);

        EnvChangeToken token = EnvChangeToken.decode(buffer);
        Redirect redirect = Redirect.decode(Unpooled.wrappedBuffer(token.getNewValue()));

        assertThat(redirect.getPort()).isEqualTo(12345);
        assertThat(redirect.getServerName()).isEqualTo("testing");
    }
}
