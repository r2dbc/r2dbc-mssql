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
import io.r2dbc.mssql.message.TDSVersion;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.Version;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Paluch
 */
final class LoginAckTokenUnitTests {

    @Test
    void shouldDecode() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("ad36000174000004164d00" + "6900630072006f0073006f0066007400"
            + "2000530051004c002000530065007200" + "760065007200000000000e000bde");

        assertThat(buffer.readByte()).isEqualTo(LoginAckToken.TYPE);

        LoginAckToken token = LoginAckToken.decode(buffer);
        assertThat(token.getTdsVersion()).isEqualTo(TDSVersion.VER_DENALI.getVersion());
        assertThat(token.getClientInterface()).isEqualTo(LoginAckToken.CLIENT_INTEFACE_TSQL);
        assertThat(token.getProgrName()).startsWith("Microsoft SQL Server");
        assertThat(token.getVersion()).isEqualTo(Version.parse("14.0.3038"));
    }
}
