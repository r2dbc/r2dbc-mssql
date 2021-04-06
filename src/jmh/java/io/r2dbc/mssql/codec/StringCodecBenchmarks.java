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
import io.r2dbc.mssql.codec.RpcParameterContext.ValueContext;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmarks for {@code StringCodec}.
 *
 * @author Mark Paluch
 */
@State(Scope.Thread)
@Testable
public class StringCodecBenchmarks extends CodecBenchmarkSupport {

    private static final DefaultCodecs codecs = new DefaultCodecs();

    private static Collation cp1252 = Collation.from(13632521, 52);

    private static final Column varchar = new Column(0, "",
        TypeInformation.builder().withCharset(ServerCharset.CP1252.charset()).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withServerType(SqlServerType.VARCHAR).build());

    private final ByteBuf varcharBuffer;

    private static Collation unicode = Collation.from(0x0445, 0);

    private static final Column nvarchar = new Column(0, "",
        TypeInformation.builder().withCharset(ServerCharset.UNICODE.charset()).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withServerType(SqlServerType.NVARCHAR).build());

    private final ByteBuf nvarcharBuffer;

    private static final Column text = new Column(0, "",
        TypeInformation.builder().withMaxLength(2147483647).withLengthStrategy(LengthStrategy.LONGLENTYPE).withServerType(SqlServerType.TEXT).withCharset(ServerCharset.CP1252.charset()).build());

    private final ByteBuf textBuffer;

    private static final Column uuid = new Column(0, "",
        TypeInformation.builder().withMaxLength(16).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withServerType(SqlServerType.GUID).build());

    private final ByteBuf uuidBuffer;

    public StringCodecBenchmarks() {

        this.varcharBuffer = alloc.buffer();
        Encode.uShort(this.varcharBuffer, 6);
        this.varcharBuffer.writeCharSequence("foobar", ServerCharset.CP1252.charset());

        this.nvarcharBuffer = alloc.buffer();
        Encode.uShort(this.nvarcharBuffer, 104);
        this.nvarcharBuffer.writeCharSequence("Γαζέες καὶ μυρτιὲς δὲν θὰ βρῶ πιὰ στὸ χρυσαφὶ ξέφωτο", ServerCharset.UNICODE.charset());

        this.textBuffer = HexUtils.decodeToByteBuf("10 64" +
            "75 6D 6D 79 20 74 65 78 74 70 74 72 00 00 00 64" +
            "75 6D 6D 79 54 53 00 0B 00 00 00 6D 79 74 65 78" +
            "74 76 61 6C 75 65");

        this.uuidBuffer = HexUtils.decodeToByteBuf("F17B0DC7C7E5C54098C7A12F7E686724FD");
    }

    @Benchmark
    public String decodeVarchar() {
        this.varcharBuffer.readerIndex(0);
        return codecs.decode(this.varcharBuffer, varchar, String.class);
    }

    @Benchmark
    public Encoded encodeVarchar() {
        return doEncode(cp1252, "foobar");
    }

    @Benchmark
    public String decodeNVarchar() {
        this.nvarcharBuffer.readerIndex(0);
        return codecs.decode(this.nvarcharBuffer, nvarchar, String.class);
    }

    @Benchmark
    public Encoded encodeNvarchar() {
        return doEncode(unicode, "Γαζέες καὶ μυρτιὲς δὲν θὰ βρῶ πιὰ στὸ χρυσαφὶ ξέφωτο");
    }

    @Benchmark
    public String decodeText() {
        this.textBuffer.readerIndex(0);
        return codecs.decode(this.textBuffer, text, String.class);
    }

    @Benchmark
    public String decodeUuid() {
        this.uuidBuffer.readerIndex(0);
        return codecs.decode(this.uuidBuffer, uuid, String.class);
    }

    @Benchmark
    public Encoded encodeNull() {
        Encoded encoded = codecs.encodeNull(TestByteBufAllocator.TEST, String.class);
        encoded.release();
        return encoded;
    }

    private Encoded doEncode(Collation collation, Object value) {
        Encoded encoded = codecs.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(ValueContext.character(collation, true)), value);
        encoded.release();
        return encoded;
    }
}
