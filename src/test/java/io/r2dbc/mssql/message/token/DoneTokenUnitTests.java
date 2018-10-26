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
 * Unit tests for {@link DoneToken}.
 *
 * @author Mark Paluch
 */
class DoneTokenUnitTests {

    @Test
    void shouldDecodeToken() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("FD1000C1000100000000000000");

        assertThat(buffer.readByte()).isEqualTo(DoneToken.TYPE);

        DoneToken token = DoneToken.decode(buffer);
        assertThat(token.isDone()).isTrue();
        assertThat(token.hasMore()).isFalse();
        assertThat(token.hasCount()).isTrue();
        assertThat(token.getRowCount()).isEqualTo(1);
    }
}
