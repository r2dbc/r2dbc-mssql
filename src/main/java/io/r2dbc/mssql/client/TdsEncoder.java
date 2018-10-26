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

package io.r2dbc.mssql.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.tds.ContextualTdsFragment;
import io.r2dbc.mssql.message.tds.FirstTdsFragment;
import io.r2dbc.mssql.message.tds.LastTdsFragment;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.message.tds.TdsPacket;
import io.r2dbc.mssql.message.token.EnvChangeToken;
import io.r2dbc.mssql.util.Assert;

/**
 * Encoder for TDS packets. This encoder can apply various strategies regarding TDS header handling:
 * <ul>
 * <li>Pass-thru {@link ByteBuf} messages (typically used for SSL traffic)</li>
 * <li>Prefix {@link ByteBuf} messages with TDS {@link Header} (typically used for SSL Handshake during PRELOGIN)</li>
 * <li>Apply {@link HeaderOptions} state for subsequent messages (typically used to set a header context until receiving
 * {@link LastTdsFragment} or {@link ResetHeader}) when initiated by a written {@link HeaderOptions} or
 * {@link FirstTdsFragment}.</li>
 * <li>Reset {@link HeaderOptions} when a {@link ResetHeader#INSTANCE ResetHeader} is written.</li>
 * </ul>
 *
 * @author Mark Paluch
 * @see FirstTdsFragment
 * @see LastTdsFragment
 * @see TdsPacket
 * @see HeaderOptions
 * @see TdsFragment
 */
public class TdsEncoder extends ChannelOutboundHandlerAdapter implements EnvironmentChangeListener {

    private final PacketIdProvider packetIdProvider;

    private int packetSize = 8000;

    private HeaderOptions headerOptions;

    public TdsEncoder(PacketIdProvider packetIdProvider) {
        this.packetIdProvider = packetIdProvider;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        if (msg == ResetHeader.INSTANCE) {

            this.headerOptions = null;
            ctx.write(Unpooled.EMPTY_BUFFER, promise);
            return;
        }

        if (msg instanceof HeaderOptions) {

            this.headerOptions = (HeaderOptions) msg;
            ctx.write(Unpooled.EMPTY_BUFFER, promise);
            return;
        }

        // Expect ByteBuf to be self-contained messages that do not require further chunking (for now).
        if (msg instanceof ByteBuf) {

            if (headerOptions == null) {
                ctx.write(msg, promise);
                return;
            }

            ByteBuf message = (ByteBuf) msg;

            doWriteFragment(ctx, promise, message, getLastHeader(this.headerOptions));
            return;
        }

        // Write entire TDSPacket
        if (msg instanceof TdsPacket) {

            TdsPacket packet = (TdsPacket) msg;
            ByteBuf message = packet.encode(ctx.alloc(), packetIdProvider);
            ctx.write(message, promise);
            return;
        }

        // Write message use HeaderOptions for subsequent packets and apply HeaderOptions
        if (msg instanceof FirstTdsFragment) {

            FirstTdsFragment fragment = (FirstTdsFragment) msg;

            this.headerOptions = fragment.getHeaderOptions();

            doWriteFragment(ctx, promise, fragment.getByteBuf(), this.headerOptions);
            return;
        }

        // Write message use HeaderOptions for subsequent packets and apply HeaderOptions
        if (msg instanceof ContextualTdsFragment) {

            ContextualTdsFragment fragment = (ContextualTdsFragment) msg;

            doWriteFragment(ctx, promise, fragment.getByteBuf(), getLastHeader(fragment.getHeaderOptions()));
            return;
        }

        // Write message, apply HeaderOptions and clear HeaderOptions
        if (msg instanceof LastTdsFragment) {

            Assert.state(this.headerOptions != null, "HeaderOptions must not be null!");

            TdsFragment fragment = (TdsFragment) msg;

            doWriteFragment(ctx, promise, fragment.getByteBuf(), getLastHeader(this.headerOptions));

            this.headerOptions = null;
            return;
        }

        // Write message and apply HeaderOptions
        if (msg instanceof TdsFragment) {

            Assert.state(this.headerOptions != null, "HeaderOptions must not be null!");

            TdsFragment fragment = (TdsFragment) msg;

            doWriteFragment(ctx, promise, fragment.getByteBuf(), headerOptions);
            return;
        }

        throw new IllegalArgumentException(String.format("Unsupported message type: %s", msg));
    }

    @Override
    public void onEnvironmentChange(EnvironmentChangeEvent event) {

        EnvChangeToken token = event.getToken();
        if (token.getChangeType() == EnvChangeToken.EnvChangeType.Packetsize) {
            this.packetSize = Integer.parseInt(token.getNewValueString());
        }
    }

    private static HeaderOptions getLastHeader(HeaderOptions headerOptions) {
        return HeaderOptions.create(headerOptions.getType(), headerOptions.getStatus().and(Status.StatusBit.EOM));
    }

    private ByteBuf addHeaderPrefix(ByteBufAllocator allocator, ByteBuf message, HeaderOptions headerOptions) {

        ByteBuf buffer = allocator.buffer(Header.SIZE + message.readableBytes());
        Header header = Header.create(headerOptions, Header.SIZE + message.readableBytes(), packetIdProvider);

        header.encode(buffer);
        buffer.writeBytes(message);

        return buffer;
    }

    private void doWriteFragment(ChannelHandlerContext ctx, ChannelPromise promise, ByteBuf byteBuf,
                                 HeaderOptions headerOptions) {

        ByteBuf buffer = addHeaderPrefix(ctx.alloc(), byteBuf, headerOptions);
        byteBuf.release();

        ctx.writeAndFlush(buffer, promise);
    }

    /**
     * Marker message to reset {@link HeaderOptions}.
     */
    public enum ResetHeader {
        INSTANCE;
    }
}
