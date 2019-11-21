/*
 * Copyright 2019 the original author or authors.
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
import io.r2dbc.mssql.codec.RpcParameterContext.CharacterValueContext;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Clob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.CharBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

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
        // TODO: NTEXT
        CharacterValueContext valueContext = context.getRequiredValueContext(CharacterValueContext.class);

        Flux<ByteBuf> binaryStream = Flux.from(value.stream()).map(it -> {

            return ByteBufUtil.encodeString(allocator, CharBuffer.wrap(it), valueContext.getCollation().getCharset());
        });

        return new PlpEncodedCharacters(SqlServerType.VARCHARMAX, valueContext.getCollation(), allocator, binaryStream, () -> Mono.from(value.discard()).toFuture());
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

        final TypeInformation type;

        private final Length valueLength;

        final ByteBuf buffer;

        ScalarClob(TypeInformation type, Length valueLength, ByteBuf buffer) {
            this.type = type;
            this.valueLength = valueLength;
            this.buffer = buffer.touch("ScalarClob");
        }

        @Override
        public Publisher<CharSequence> stream() {

            return Flux.<CharSequence>generate(sink -> {

                try {
                    if (!this.buffer.isReadable()) {
                        sink.complete();
                        return;
                    }

                    Length length;
                    if (this.type.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {
                        length = Length.decode(this.buffer, this.type);
                    } else {
                        length = this.valueLength;
                    }

                    String value = this.buffer.toString(this.buffer.readerIndex(), length.getLength(), this.type.getCharset());
                    this.buffer.skipBytes(length.getLength());

                    sink.next(value);
                } catch (Exception e) {
                    sink.error(e);
                }
            }).doOnCancel(() -> {
                if (this.buffer.refCnt() > 0) {
                    this.buffer.release();
                }
            })
                .doAfterTerminate(() -> {

                    if (this.buffer.refCnt() > 0) {
                        this.buffer.release();
                    }
                });
        }

        @Override
        public Publisher<Void> discard() {

            return Mono.fromRunnable(() -> {

                if (this.buffer.refCnt() > 0) {
                    this.buffer.release();
                }
            });
        }
    }
}
