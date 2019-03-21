/*
 * Copyright 2018 the original author or authors.
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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

import java.util.UUID;

/**
 * @author Mark Paluch
 */
final class UuidCodec extends AbstractCodec<UUID> {

    /**
     * Singleton instance.
     */
    static final UuidCodec INSTANCE = new UuidCodec();

    private UuidCodec() {
        super(UUID.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, UUID value) {

        return RpcEncoding.encode(allocator, SqlServerType.GUID, 16, value, (buffer, uuid) -> {

            long msb = value.getMostSignificantBits();
            long lsb = value.getLeastSignificantBits();

            buffer.writeBytes(swapForWrite(msb));
            buffer.writeLong(lsb);
        });
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.encodeNull(allocator, SqlServerType.GUID);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == SqlServerType.GUID;
    }

    @Override
    UUID doDecode(ByteBuf buffer, Length length, TypeInformation typeInformation, Class<? extends UUID> valueType) {

        if (length.isNull()) {
            return null;
        }

        byte[] bytes = new byte[8];
        buffer.readBytes(bytes);

        long msb = swapForRead(bytes);
        long lsb = buffer.readLong();

        return new UUID(msb, lsb);
    }

    /**
     * Swap bytes. MSB is represented in order 3,2,1,0 and 5,4,7,6
     *
     * @param memory
     * @return
     */
    private static long swapForRead(byte[] memory) {
        return ((long) memory[3] & 0xff) << 56 |
            ((long) memory[2] & 0xff) << 48 |
            ((long) memory[1] & 0xff) << 40 |
            ((long) memory[0] & 0xff) << 32 |
            ((long) memory[5] & 0xff) << 24 |
            ((long) memory[4] & 0xff) << 16 |
            ((long) memory[7] & 0xff) << 8 |
            (long) memory[6] & 0xff;
    }

    /**
     * Swap bytes. MSB is represented in order 3,2,1,0 and 5,4,7,6
     *
     * @param msb
     * @return
     */
    private byte[] swapForWrite(long msb) {

        byte[] bytes = new byte[8];
        bytes[3] = (byte) ((msb >> 56) & 0xff);
        bytes[2] = (byte) ((msb >> 48) & 0xff);
        bytes[1] = (byte) ((msb >> 40) & 0xff);
        bytes[0] = (byte) ((msb >> 32) & 0xff);

        bytes[5] = (byte) ((msb >> 24) & 0xff);
        bytes[4] = (byte) ((msb >> 16) & 0xff);
        bytes[7] = (byte) ((msb >> 8) & 0xff);
        bytes[6] = (byte) ((msb) & 0xff);

        return bytes;
    }
}
