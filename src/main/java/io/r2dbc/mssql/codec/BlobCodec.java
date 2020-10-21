/*
 * Copyright 2019-2020 the original author or authors.
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
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Blob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Codec for binary values that are represented as {@link Blob}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#BINARY},  {@link SqlServerType#VARBINARY}, {@link SqlServerType#VARBINARYMAX}, and {@link SqlServerType#IMAGE}.</li>
 * <li>Java type: {@link Blob}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
public class BlobCodec extends AbstractCodec<Blob> {

    /**
     * Singleton instance.
     */
    public static final BlobCodec INSTANCE = new BlobCodec();

    private static final Set<SqlServerType> SUPPORTED_TYPES = EnumSet.of(SqlServerType.BINARY, SqlServerType.VARBINARY,
        SqlServerType.VARBINARYMAX, SqlServerType.IMAGE);

    private BlobCodec() {
        super(Blob.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Blob value) {
        return new PlpEncoded(SqlServerType.VARBINARYMAX, allocator, Flux.from(value.stream()).map(Unpooled::wrappedBuffer), () -> Mono.from(value.discard()).toFuture());
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return BinaryCodec.INSTANCE.encodeNull(allocator);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return SUPPORTED_TYPES.contains(typeInformation.getServerType());
    }

    @Nullable
    public Blob decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends Blob> type) {

        Assert.requireNonNull(decodable, "Decodable must not be null");
        Assert.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        Length length;

        if (decodable.getType().getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            PlpLength plpLength = PlpLength.decode(buffer, decodable.getType());
            length = Length.of(Math.toIntExact(plpLength.getLength()), plpLength.isNull());
        } else {
            length = Length.decode(buffer, decodable.getType());
        }

        if (length.isNull()) {
            return null;
        }

        return doDecode(buffer, length, decodable.getType(), type);
    }

    @Override
    Blob doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Blob> valueType) {

        if (length.isNull()) {
            return null;
        }

        if (type.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            List<ByteBuf> chunks = new ArrayList<>();
            while (buffer.isReadable()) {

                Length chunkLength = Length.decode(buffer, type);
                chunks.add(buffer.readRetainedSlice(chunkLength.getLength()));
            }

            return new ScalarBlob(chunks);
        }

        return new ScalarBlob(Collections.singletonList(buffer.readRetainedSlice(length.getLength())));
    }

    /**
     * Scalar {@link Blob} backed by an already received and de-chunked {@link List} of {@link ByteBuf}.
     */
    static class ScalarBlob implements Blob {

        final List<ByteBuf> buffers;

        ScalarBlob(List<ByteBuf> buffers) {
            this.buffers = buffers;
            this.buffers.forEach(byteBuf -> byteBuf.touch("ScalarBlob"));
        }

        @Override
        public Publisher<ByteBuffer> stream() {

            return Flux.fromIterable(this.buffers).map(it -> {

                if (!it.isReadable()) {
                    it.release();
                    return ByteBuffer.wrap(new byte[0]);
                }

                ByteBuffer result = ByteBuffer.allocate(it.readableBytes());
                it.readBytes(result);
                it.release();

                result.flip();
                return result;
            })
                .doOnDiscard(ByteBuf.class, ByteBuf::release)
                .doOnCancel(() -> {

                    for (ByteBuf buffer : this.buffers) {
                        if (buffer.refCnt() > 0) {
                            buffer.release();
                        }
                    }
                });
        }

        @Override
        public Publisher<Void> discard() {

            return Mono.fromRunnable(() -> {

                for (ByteBuf buffer : this.buffers) {
                    if (buffer.refCnt() > 0) {
                        buffer.release();
                    }
                }
            });
        }

    }

}
