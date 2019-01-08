/*
 * Copyright 2018-2019 the original author or authors.
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
 * Unit tests for {@link TabnameToken}.
 *
 * @author Mark Paluch
 */
class TabnameTokenUnitTests {

    @Test
    void shouldDecodeToken() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("a413000108006d0079005f007400" +
            "610062006c006500");

        assertThat(buffer.readByte()).isEqualTo(TabnameToken.TYPE);

        TabnameToken token = TabnameToken.decode(buffer);
        assertThat(token.getTableNames()).hasSize(1);

        Identifier name = token.getTableNames().get(0);
        assertThat(name.getObjectName()).isEqualTo("my_table");
    }

    @Test
    void canDecodeShouldReportDecodability() {

        String data = "13000108006d0079005f007400" +
            "610062006c006500";

        CanDecodeTestSupport.testCanDecode(HexUtils.decodeToByteBuf(data), TabnameToken::canDecode);
    }
}
