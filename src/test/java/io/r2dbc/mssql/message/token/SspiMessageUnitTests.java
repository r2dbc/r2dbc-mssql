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

import io.netty.buffer.ByteBufUtil;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.ContextualTdsFragment;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SspiMessage}.
 *
 * @author Daniel Pachali
 */
final class SspiMessageUnitTests {

    @Test
    void shouldEncodeSspiPacket() {

        byte[] sspiBuffer = new byte[]{0x01, 0x02, 0x03, 0x04};

        SspiMessage message = SspiMessage.create(sspiBuffer);
        ContextualTdsFragment fragment =
            (ContextualTdsFragment) message.encode(TestByteBufAllocator.TEST, 0);

        assertThat(fragment.getHeaderOptions().getType()).isEqualTo(Type.SSPI);
        assertThat(fragment.getHeaderOptions().getStatus().getValue()).isEqualTo((byte) 0);
        assertThat(ByteBufUtil.getBytes(fragment.getByteBuf())).containsExactly(sspiBuffer);
    }

    @Test
    void shouldEncodeRawSspiPayloadWithoutTokenHeader() {

        byte[] sspiBuffer = new byte[]{(byte) 0x60, (byte) 0x82, 0x01, 0x23};

        SspiMessage message = SspiMessage.create(sspiBuffer);
        ContextualTdsFragment fragment =
            (ContextualTdsFragment) message.encode(TestByteBufAllocator.TEST, 0);

        assertThat(ByteBufUtil.getBytes(fragment.getByteBuf()))
            .containsExactly((byte) 0x60, (byte) 0x82, (byte) 0x01, (byte) 0x23);
    }

    @Test
    void shouldExposeSymbolicName() {

        SspiMessage message = SspiMessage.create(new byte[]{0x01});

        assertThat(message.getName()).isEqualTo("SSPI");
        assertThat(message.toString()).contains("length=1");
    }

}
