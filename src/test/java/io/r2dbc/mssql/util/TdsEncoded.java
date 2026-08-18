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

package io.r2dbc.mssql.util;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.TypeInformation;

import java.util.Arrays;

/**
 * Utility to create test data in TDS wire format.
 *
 * @author Mark Paluch
 */
public final class TdsEncoded {

    private TdsEncoded() {
    }

    /**
     * Create a PLP stream carrying {@code data} split into chunks of the given sizes. Chunk sizes must sum up to {@code data.length}.
     * Omitting chunk sizes writes {@code data} as a single chunk.
     *
     * @param type       the type descriptor providing the length strategy.
     * @param data       the payload to write.
     * @param chunkSizes sizes of the individual chunks.
     * @return the buffer containing the PLP stream.
     */
    public static ByteBuf plpStream(TypeInformation type, byte[] data, int... chunkSizes) {
        return plpStream(TestByteBufAllocator.TEST.buffer(), type, data, chunkSizes);
    }

    /**
     * Create a PLP stream writing each string as its own chunk using the default charset.
     *
     * @param type   the type descriptor providing the length strategy.
     * @param chunks the chunks to write.
     * @return the buffer containing the PLP stream.
     */
    public static ByteBuf plpStream(TypeInformation type, String... chunks) {
        return plpStream(TestByteBufAllocator.TEST.buffer(), type, chunks);
    }

    /**
     * Create a PLP stream in {@code buffer} carrying {@code data} split into chunks of the given sizes.
     * Chunk sizes must sum up to {@code data.length}. Omitting chunk sizes writes {@code data} as a single chunk.
     *
     * @param buffer     the target buffer.
     * @param type       the type descriptor providing the length strategy.
     * @param data       the payload to write.
     * @param chunkSizes sizes of the individual chunks.
     * @return {@code buffer} containing the PLP stream.
     */
    public static ByteBuf plpStream(ByteBuf buffer, TypeInformation type, byte[] data, int... chunkSizes) {

        if (chunkSizes.length == 0) {
            chunkSizes = new int[]{data.length};
        }

        int total = Arrays.stream(chunkSizes).sum();
        Assert.isTrue(total == data.length, () -> String.format("Chunk sizes sum up to %d bytes, data has %d bytes", total, data.length));

        PlpLength.of(data.length).encode(buffer);

        int offset = 0;
        for (int chunkSize : chunkSizes) {
            Length.of(chunkSize).encode(buffer, type);
            buffer.writeBytes(data, offset, chunkSize);
            offset += chunkSize;
        }

        return buffer;
    }

    /**
     * Create a PLP stream in {@code buffer} writing each string as its own chunk using the default charset.
     *
     * @param buffer the target buffer.
     * @param type   the type descriptor providing the length strategy.
     * @param chunks the chunks to write.
     * @return {@code buffer} containing the PLP stream.
     */
    public static ByteBuf plpStream(ByteBuf buffer, TypeInformation type, String... chunks) {

        int[] chunkSizes = new int[chunks.length];
        StringBuilder data = new StringBuilder();

        for (int i = 0; i < chunks.length; i++) {
            chunkSizes[i] = chunks[i].getBytes().length;
            data.append(chunks[i]);
        }

        return plpStream(buffer, type, data.toString().getBytes(), chunkSizes);
    }

    /**
     * Encode a {@code B_VARCHAR} (unicode string prefixed with its byte-sized length).
     *
     * @param buffer the target buffer.
     * @param value  the string to write.
     */
    public static void encodeUnicodeBString(ByteBuf buffer, String value) {

        Encode.asByte(buffer, value.length());
        Encode.rpcString(buffer, value);
    }

}
