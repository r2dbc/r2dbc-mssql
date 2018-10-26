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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LoginAckToken}.
 *
 * @author Mark Paluch
 */
final class TabularUnitTests {

    @Test
    @SuppressWarnings("unchecked")
    void shouldDecodeLoginAck() {

        String hex = "e31b0001066d0061" + "007300740065007200066d0061007300"
            + "740065007200ab700045160000020025" + "004300680061006e0067006500640020" + "00640061007400610062006100730065"
            + "00200063006f006e0074006500780074" + "00200074006f00200027006d00610073" + "0074006500720027002e000c61003600"
            + "38003800300039003200610037003900" + "660035000001000000e3080007050904" + "d0003400e31700020a750073005f0065"
            + "006e0067006c0069007300680000ab74" + "0047160000010027004300680061006e" + "0067006500640020006c0061006e0067"
            + "00750061006700650020007300650074" + "00740069006e006700200074006f0020" + "00750073005f0065006e0067006c0069"
            + "00730068002e000c6100360038003800" + "30003900320061003700390066003500" + "0001000000ad36000174000004164d00"
            + "6900630072006f0073006f0066007400" + "2000530051004c002000530065007200" + "760065007200000000000e000bdee313"
            + "00040438003000300030000434003000" + "39003600ae040100000001fffd000000" + "000000000000000000";

        ByteBuf byteBuf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(hex));

        Tabular tabular = Tabular.decode(byteBuf, false);

        assertThat(tabular.getTokens()).hasSize(9);
        assertThat(tabular.getRequiredToken(LoginAckToken.class)).isNotNull();
        assertThat(tabular
            .getRequiredToken(EnvChangeToken.class, it -> it.getChangeType() == EnvChangeToken.EnvChangeType.Database)
            .getNewValueString()).isEqualTo("master");
    }
}
