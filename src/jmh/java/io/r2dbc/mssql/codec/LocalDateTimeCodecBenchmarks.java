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
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.time.LocalDateTime;

/**
 * Benchmarks for {@code LocalDateTimeCodec}.
 *
 * @author Mark Paluch
 */
@State(Scope.Thread)
@Testable
public class LocalDateTimeCodecBenchmarks extends CodecBenchmarkSupport {

    private static final DefaultCodecs codecs = new DefaultCodecs();

    private static final Column smallDateTimeColumn = new Column(0, "",
        TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.SMALLDATETIME).build());

    private final ByteBuf smallDateTimeBuffer = HexUtils.decodeToByteBuf("04 F5 A8 3E 00");

    private static final Column dateTimeColumn = new Column(0, "",
        TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.DATETIME).build());

    private final ByteBuf dateTimeBuffer = HexUtils.decodeToByteBuf("08 86 A9 00 00 AA 70 02 01");

    private static final Column dateTime2Column = new Column(0, "",
        TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.DATETIME2).withScale(7).build());

    private final ByteBuf dateTime2Buffer = HexUtils.decodeToByteBuf("082006E17483E13E0B");

    private final LocalDateTime toEncode = LocalDateTime.parse("2018-06-04T01:02");

    @Benchmark
    public Object decodeSmallDateTime() {
        this.smallDateTimeBuffer.readerIndex(0);
        return codecs.decode(this.smallDateTimeBuffer, smallDateTimeColumn, LocalDateTime.class);
    }

    @Benchmark
    public Object decodeDateTime() {
        this.dateTimeBuffer.readerIndex(0);
        return codecs.decode(this.dateTimeBuffer, dateTimeColumn, LocalDateTime.class);
    }


    @Benchmark
    public Object decodeDateTime2() {
        this.dateTime2Buffer.readerIndex(0);
        return codecs.decode(this.dateTime2Buffer, dateTime2Column, LocalDateTime.class);
    }

    @Benchmark
    public Encoded encode() {
        return doEncode(this.toEncode);
    }

    @Benchmark
    public Encoded encodeNull() {
        Encoded encoded = codecs.encodeNull(alloc, LocalDateTime.class);
        encoded.release();
        return encoded;
    }

    private Encoded doEncode(Object value) {
        Encoded encoded = codecs.encode(alloc, RpcParameterContext.in(), value);
        encoded.release();
        return encoded;
    }
}
