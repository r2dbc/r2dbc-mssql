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
import io.netty.channel.embedded.EmbeddedChannel;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;

import java.util.Queue;

/**
 * Assertion utility for {@link EmbeddedChannel} to verify channel interaction.
 *
 * @author Mark Paluch
 */
public final class EmbeddedChannelAssert extends AbstractObjectAssert<EmbeddedChannelAssert, EmbeddedChannel> {

    private EmbeddedChannelAssert(EmbeddedChannel actual) {
        super(actual, EmbeddedChannelAssert.class);
    }

    /**
     * Create an assertion for an {@link EmbeddedChannel}.
     *
     * @param actual the channel.
     */
    public static EmbeddedChannelAssert assertThat(EmbeddedChannel actual) {
        return new EmbeddedChannelAssert(actual);
    }

    /**
     * Assert inbound (received) messages.
     */
    public MessagesAssert inbound() {
        return new MessagesAssert("inbound", this.actual.inboundMessages());
    }

    /**
     * Assert outbound (sent) messages.
     */
    public MessagesAssert outbound() {
        return new MessagesAssert("outbound", this.actual.outboundMessages());
    }

    /**
     * Assertions for messages of the embedded channel.
     */
    public static final class MessagesAssert extends AbstractObjectAssert<MessagesAssert, Queue<Object>> {

        private final String direction;

        private MessagesAssert(String direction, Queue<Object> actual) {
            super(actual, MessagesAssert.class);
            this.direction = direction;
        }

        /**
         * Assert that the message contains {@link ByteBuf} messages.
         */
        public EncodedAssert hasByteBufMessage() {

            isNotNull();
            Object poll = actual.poll();

            Assertions.assertThat(poll).describedAs(this.direction + " message").isNotNull().isInstanceOf(ByteBuf.class);

            return new EncodedAssert((ByteBuf) poll);
        }
    }
}
