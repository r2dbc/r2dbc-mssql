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
import io.r2dbc.mssql.message.token.FeatureExtAckToken.ColumnEncryption;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FeatureExtAckToken}.
 *
 * @author Mark Paluch
 */
class FeatureExtAckTokenUnitTests {

    @Test
    void shouldDecode() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("ae040100000001ff");

        assertThat(buffer.readByte()).isEqualTo(FeatureExtAckToken.TYPE);

        FeatureExtAckToken token = FeatureExtAckToken.decode(buffer);

        List<FeatureExtAckToken.FeatureToken> tokens = token.getFeatureTokens();
        assertThat(tokens).hasSize(1);

        ColumnEncryption encryption = (ColumnEncryption) tokens.get(0);

        assertThat(encryption.getTceVersion()).isEqualTo((byte) 1);
    }
}
