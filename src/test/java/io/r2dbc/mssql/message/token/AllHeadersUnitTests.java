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
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AllHeaders}.
 *
 * @author Mark Paluch
 */
class AllHeadersUnitTests {

    @Test
    void shouldEncodeProperly() {

        byte tx[] = new byte[]{0, 0, 0, 0, 0, 0, 0, 0};
        AllHeaders.TransactionDescriptorHeader txDescriptor = new AllHeaders.TransactionDescriptorHeader(tx, 1);

        AllHeaders header = new AllHeaders(Collections.singletonList(txDescriptor));

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();

        header.encode(buffer);

        assertThat(buffer.readableBytes()).isEqualTo(header.getLength());
        assertThat(ByteBufUtil.hexDump(buffer)).isEqualTo("16000000120000000200000000000000000001000000");
    }
}
