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

package io.r2dbc.mssql.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.mssql.codec.Encoded;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;

import java.nio.charset.Charset;
import java.util.function.Consumer;

/**
 * Assertions for {@link ByteBuf}.
 *
 * @author Mark Paluch
 */
public final class EncodedAssert extends AbstractObjectAssert<EncodedAssert, ByteBuf> {

    EncodedAssert(ByteBuf actual) {
        super(actual, EncodedAssert.class);
    }

    /**
     * Create an assertion for a {@link ByteBuf}.
     *
     * @param actual
     */
    public static EncodedAssert assertThat(ByteBuf actual) {
        return new EncodedAssert(actual);
    }

    /**
     * Create an assertion for a {@link ByteBuf}.
     *
     * @param actual
     */
    public static EncodedAssert assertThat(Encoded actual) {
        return new EncodedAssert(actual.getValue());
    }

    /**
     * Assert that the actual {@link ByteBuf data buffer} contains the expected string by converting the buffer using the default {@link Charset}.
     *
     * @param expected the expected string.
     */
    public EncodedAssert contains(String expected) {
        return contains(expected, Charset.defaultCharset());
    }

    /**
     * Assert that the actual {@link ByteBuf data buffer} contains the expected string by converting the buffer using the given {@link Charset}.
     *
     * @param expected the expected string.
     * @param charset  the charset to use.
     */
    public EncodedAssert contains(String expected, Charset charset) {

        isNotNull();

        String actual = this.actual.toString(charset);

        Assertions.assertThat(actual).contains(expected);

        return this;
    }

    /**
     * Assert that the actual {@link ByteBuf data buffer} is equal to the expected hexadecimal representation.
     *
     * @param expected the expected value represented as hexadecimal characters.
     */
    public EncodedAssert isEqualToHex(String expected) {

        isNotNull();

        Assertions.assertThat(ByteBufUtil.prettyHexDump(actual)).describedAs("ByteBuf")
            .isEqualTo(ByteBufUtil.prettyHexDump(HexUtils.decodeToByteBuf(expected)));

        return this;
    }

    /**
     * Assert that the actual {@link ByteBuf data buffer} is empty (i.e. contains no readable bytes).
     */
    public void isEmpty() {
        Assertions.assertThat(this.actual.readableBytes()).isEqualTo(0);
    }

    /**
     * Assert that the actual {@link ByteBuf data buffer} is encoded as described by the expected encoded buffer. The expected value is passed to a {@link Consumer} that writes the expectation to
     * the {@link ByteBuf data
     * buffer}.
     *
     * @param encoded expectation writer.
     */
    public EncodedAssert isEncodedAs(Consumer<ByteBuf> encoded) {

        isNotNull();

        ByteBuf expected = TestByteBufAllocator.TEST.buffer();
        encoded.accept(expected);

        Assertions.assertThat(ByteBufUtil.prettyHexDump(actual)).describedAs("ByteBuf")
            .isEqualTo(ByteBufUtil.prettyHexDump(expected));

        return this;
    }
}
