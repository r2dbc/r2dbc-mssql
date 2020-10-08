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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.mssql.codec.RpcParameterContext.CharacterValueContext;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.R2dbcNonTransientException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Codec for character values that are represented as {@link Clob}.
 * <p>
 * BlobCodec
 * <li>Server types: {@link SqlServerType#CHAR}, {@link SqlServerType#NCHAR}, {@link SqlServerType#VARCHAR}, {@link SqlServerType#NVARCHAR}, {@link SqlServerType#VARCHARMAX},
 * {@link SqlServerType#NVARCHARMAX}, {@link SqlServerType#TEXT} and {@link SqlServerType#NTEXT}.</li>
 * <li>Java type: {@link Clob}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
public class ClobCodec extends AbstractCodec<Clob> {

    /**
     * Singleton instance.
     */
    public static final ClobCodec INSTANCE = new ClobCodec();

    private static final Set<SqlServerType> SUPPORTED_TYPES = EnumSet.of(SqlServerType.CHAR, SqlServerType.NCHAR,
        SqlServerType.VARCHAR, SqlServerType.NVARCHAR,
        SqlServerType.VARCHARMAX, SqlServerType.NVARCHARMAX,
        SqlServerType.TEXT, SqlServerType.NTEXT);

    private ClobCodec() {
        super(Clob.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Clob value) {
        return CharacterEncoder.encodePlp(allocator, context.getRequiredValueContext(CharacterValueContext.class), value);
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return StringCodec.INSTANCE.doEncodeNull(allocator);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return SUPPORTED_TYPES.contains(typeInformation.getServerType());
    }

    @Nullable
    public Clob decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends Clob> type) {

        Assert.requireNonNull(decodable, "Decodable must not be null");
        Assert.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        Length length;

        if (decodable.getType().getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            PlpLength plpLength = buffer.isReadable() ? PlpLength.decode(buffer, decodable.getType()) : PlpLength.nullLength();
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
    Clob doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Clob> valueType) {

        if (length.isNull()) {
            return null;
        }

        if (type.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            int startIndex = buffer.readerIndex();
            while (buffer.isReadable()) {

                Length chunkLength = Length.decode(buffer, type);
                buffer.skipBytes(chunkLength.getLength());
            }

            int endIndex = buffer.readerIndex();
            buffer.readerIndex(startIndex);
            return new ScalarClob(type, length, buffer.readRetainedSlice(endIndex - startIndex));
        }

        return new ScalarClob(type, length, buffer.readRetainedSlice(length.getLength()));
    }

    /**
     * Scalar {@link Clob} backed by an already received and de-chunked {@link List} of {@link ByteBuf}.
     */
    static class ScalarClob implements Clob {

        private final TypeInformation type;

        private final Length valueLength;

        private final ByteBuf buffer;

        private final CompositeByteBuf remainder;

        ScalarClob(TypeInformation type, Length valueLength, ByteBuf buffer) {
            this.type = type;
            this.valueLength = valueLength;
            this.buffer = buffer.touch("ScalarClob");
            this.remainder = buffer.alloc().compositeBuffer();
        }

        @Override
        public Publisher<CharSequence> stream() {

            CharsetDecoder decoder = this.type.getCharset().newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);

            AtomicReference<CoderResult> result = new AtomicReference<>();
            AtomicInteger counter = new AtomicInteger();
            return createBufferStream(this.buffer, this.valueLength, this.type).<CharSequence>handle((buffer, sink) -> {

                if (!buffer.isReadable()) {
                    // ensure release if not consumed
                    buffer.release();
                    return;
                }

                this.remainder.addComponent(true, buffer);
                ByteBuffer byteBuffer = this.remainder.nioBuffer();

                int size = byteBuffer.remaining();
                CharBuffer outBuffer = CharBuffer.allocate(byteBuffer.remaining());

                CoderResult decode;
                synchronized (decoder) {
                    decode = decoder.decode(byteBuffer, outBuffer, false);
                }

                result.set(decode);
                int consumed = size - byteBuffer.remaining();

                if (consumed > 0) {
                    this.remainder.skipBytes(consumed);
                } else {
                    sink.error(new MalformedInputException(consumed));
                    return;
                }

                if (counter.incrementAndGet() % 16 == 0) {
                    this.remainder.discardSomeReadBytes();
                }

                outBuffer.flip();
                sink.next(outBuffer.toString());
            }).doOnComplete(() -> {

                CoderResult coderResult = result.get();

                if (coderResult != null && coderResult.isError()) {

                    if (coderResult.isMalformed()) {
                        throw new ClobDecodeException("Cannot decode CLOB data. Malformed character input");
                    }
                    if (coderResult.isUnmappable()) {
                        throw new ClobDecodeException("Cannot decode CLOB data. Unmappable characters");
                    }
                }

                if (this.remainder.isReadable()) {
                    throw new ClobDecodeException("Cannot decode CLOB data. Buffer has remainder: " + ByteBufUtil.hexDump(this.remainder));
                }

            })
                .doFinally(s -> {
                    ReferenceCountUtil.safeRelease(this.remainder);
                });
        }

        @Override
        public Publisher<Void> discard() {
            return Mono.fromRunnable(this::releaseBuffers);
        }

        private void releaseBuffers() {

            ReferenceCountUtil.safeRelease(this.remainder);
            ReferenceCountUtil.safeRelease(this.buffer);
        }

        private static Flux<ByteBuf> createBufferStream(ByteBuf plpStream, Length valueLength, TypeInformation type) {

            return Flux.<ByteBuf>generate(sink -> {

                try {
                    if (!plpStream.isReadable()) {
                        sink.complete();
                        return;
                    }

                    Length length;
                    if (type.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {
                        length = Length.decode(plpStream, type);
                    } else {
                        length = valueLength;
                    }

                    sink.next(plpStream.readRetainedSlice(length.getLength()));
                } catch (Exception e) {
                    sink.error(e);
                }
            })
                .doFinally(s -> {
                    ReferenceCountUtil.safeRelease(plpStream);
                });
        }

    }

    static class ClobDecodeException extends R2dbcNonTransientException {

        public ClobDecodeException(String reason) {
            super(reason);
        }

    }

}
