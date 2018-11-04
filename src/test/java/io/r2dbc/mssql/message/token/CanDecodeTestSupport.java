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

import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Support class to test {@literal canDecode} methods.
 *
 * @author Mark Paluch
 */
final class CanDecodeTestSupport {

    /**
     * Test {@literal canDecode} method with a message without the type byte. This test method tests positive and negative decodability scenarios by truncating the data buffer until it does not
     * contain any readable bytes.
     *
     * @param buffer    data buffer containing the message without the type byte.
     * @param canDecode the {@literal canDecode} method to test.
     */
    public static void testCanDecode(ByteBuf buffer, Predicate<ByteBuf> canDecode) {

        int expectedReadIndex = buffer.readerIndex();
        int writerIndex = buffer.writerIndex();
        assertThat(canDecode).accepts(buffer);
        assertThat(buffer.readerIndex()).describedAs("readIndex after first canDecode()").isEqualTo(expectedReadIndex);

        for (int i = 1; i < writerIndex; i++) {

            buffer.writerIndex(writerIndex - i);
            assertThat(canDecode).describedAs("canDecode() with missing " + i + " bytes").rejects(buffer);
        }
    }
}
