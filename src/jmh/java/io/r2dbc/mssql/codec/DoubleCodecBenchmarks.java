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
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmarks for {@code DoubleCodec}.
 *
 * @author Mark Paluch
 */
@State(Scope.Thread)
@Testable
public class DoubleCodecBenchmarks extends CodecBenchmarkSupport {

    private static final DefaultCodecs codecs = new DefaultCodecs();

    private static final Column column = new Column(0, "",
        TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.FLOAT).withMaxLength(8).build());

    private final ByteBuf doubleBuffer = HexUtils.decodeToByteBuf("08 FE D4 78 E9 46 28 C6 40");

    private final ByteBuf floatBuffer = HexUtils.decodeToByteBuf("04 04 37 42 31 46");

    @Benchmark
    public Object decodeDouble() {
        this.doubleBuffer.readerIndex(0);
        return codecs.decode(this.doubleBuffer, column, Double.class);
    }

    @Benchmark
    public Object decodeFloat() {
        this.floatBuffer.readerIndex(0);
        return codecs.decode(this.floatBuffer, column, Double.class);
    }

    @Benchmark
    public Encoded encode() {
        return doEncode(Double.MIN_VALUE);
    }

    @Benchmark
    public Encoded encodeNull() {
        Encoded encoded = codecs.encodeNull(alloc, Double.class);
        encoded.release();
        return encoded;
    }

    private Encoded doEncode(Object value) {
        Encoded encoded = codecs.encode(alloc, RpcParameterContext.in(), value);
        encoded.release();
        return encoded;
    }
}
