/*
 * Copyright 2019-2021 the original author or authors.
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
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.nio.ByteBuffer;

/**
 * Benchmarks for {@code BinaryCodec}.
 *
 * @author Mark Paluch
 */
@State(Scope.Thread)
@Testable
public class BinaryCodecBenchmarks extends CodecBenchmarkSupport {

    private static final DefaultCodecs codecs = new DefaultCodecs();

    private static final Column binary = new Column(0, "",
        TypeInformation.builder().withLengthStrategy(LengthStrategy.USHORTLENTYPE).withServerType(SqlServerType.VARBINARY).build());

    private final ByteBuf binaryBuffer = HexUtils.decodeToByteBuf("03 00 62 61 72");

    private final byte[] toEncode = "foo".getBytes();

    private final ByteBuffer bufferToEncode = ByteBuffer.wrap(this.toEncode);

    @Benchmark
    public Object decodeToByteArray() {
        this.binaryBuffer.readerIndex(0);
        return codecs.decode(this.binaryBuffer, binary, byte[].class);
    }

    @Benchmark
    public Object decodeToByteBuffer() {
        this.binaryBuffer.readerIndex(0);
        return codecs.decode(this.binaryBuffer, binary, ByteBuffer.class);
    }

    @Benchmark
    public Encoded encodeByteArray() {
        return doEncode(this.toEncode);
    }

    @Benchmark
    public Encoded encodeByteBuffer() {
        this.bufferToEncode.rewind();
        return doEncode(this.bufferToEncode);
    }

    @Benchmark
    public Encoded encodeNull() {
        Encoded encoded = codecs.encodeNull(alloc, byte[].class);
        encoded.release();
        return encoded;
    }

    private Encoded doEncode(Object value) {
        Encoded encoded = codecs.encode(alloc, RpcParameterContext.in(), value);
        encoded.release();
        return encoded;
    }
}
