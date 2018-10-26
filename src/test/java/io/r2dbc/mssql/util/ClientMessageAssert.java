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
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.tds.ContextualTdsFragment;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.message.tds.TdsPacket;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.function.Consumer;

/**
 * Assertion utility for {@link ClientMessage}.
 *
 * @author Mark Paluch
 */
public final class ClientMessageAssert extends AbstractObjectAssert<ClientMessageAssert, ClientMessage> {

    private ClientMessageAssert(ClientMessage actual) {
        super(actual, ClientMessageAssert.class);
    }

    /**
     * Create an assertion for a {@link ClientMessage}.
     *
     * @param actual
     */
    public static ClientMessageAssert assertThat(ClientMessage actual) {
        return new ClientMessageAssert(actual);
    }

    /**
     * Assert the encoded form of {@link ClientMessage}.
     *
     * @see TdsFragment
     */
    public TdsFragmentAssert encoded() {
        return new TdsFragmentAssert(Mono.from(actual.encode(TestByteBufAllocator.TEST)).block());
    }

    /**
     * Assertions for {@link TdsFragment}.
     */
    public static class TdsFragmentAssert extends AbstractObjectAssert<TdsFragmentAssert, TdsFragment> {

        private TdsFragmentAssert(TdsFragment actual) {
            super(actual, TdsFragmentAssert.class);
        }

        /**
         * Assert that the actual {@link TdsFragment message} contains the expected string by converting the message using the default {@link Charset}.
         *
         * @param expected the expected string
         */
        public TdsFragmentAssert contains(String expected) {
            return contains(expected, Charset.defaultCharset());
        }

        /**
         * Assert that the actual {@link TdsFragment message} contains the expected string by converting the message using the given {@link Charset}.
         *
         * @param expected the expected string
         */
        public TdsFragmentAssert contains(String expected, Charset charset) {

            isNotNull();

            new EncodedAssert(actual.getByteBuf()).contains(expected, charset);

            return this;
        }

        /**
         * Assert that the actual {@link TdsFragment message} is empty.
         */
        public TdsFragmentAssert isEmpty() {

            Assertions.assertThat(this.actual.getByteBuf().readableBytes()).isEqualTo(0);

            return this;
        }

        /**
         * Assert that the actual {@link TdsFragment message} declares the expected {@link HeaderOptions}
         *
         * @param expected the expected {@link HeaderOptions}.
         */
        public TdsFragmentAssert hasHeader(HeaderOptions expected) {

            isNotNull();

            Assertions.assertThat(actual).isInstanceOfAny(ContextualTdsFragment.class, TdsPacket.class);

            if (actual instanceof ContextualTdsFragment) {

                ContextualTdsFragment contextual = (ContextualTdsFragment) actual;

                Assertions.assertThat(contextual.getHeaderOptions().getType()).isEqualTo(expected.getType());
                Assertions.assertThat(contextual.getHeaderOptions().getStatus()).isEqualTo(expected.getStatus());
            }

            return this;
        }

        /**
         * Assert that the actual {@link TdsFragment message} is encoded as described by the expected encoded buffer. The expected value is passed to a {@link Consumer} that writes the expectation
         * to the {@link ByteBuf data
         * buffer}.
         *
         * @param encoded expectation writer.
         */
        public TdsFragmentAssert isEncodedAs(Consumer<ByteBuf> encoded) {

            isNotNull();

            ByteBuf expected = TestByteBufAllocator.TEST.buffer();
            encoded.accept(expected);

            Assertions.assertThat(ByteBufUtil.prettyHexDump(actual.getByteBuf())).describedAs("ByteBuf")
                .isEqualTo(ByteBufUtil.prettyHexDump(expected));

            return this;
        }
    }
}
