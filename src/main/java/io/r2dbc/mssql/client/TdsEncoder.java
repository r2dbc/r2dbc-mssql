/*
 * Copyright 2018-2021 the original author or authors.
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

package io.r2dbc.mssql.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.PromiseCombiner;
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
public final class TdsEncoder extends ChannelOutboundHandlerAdapter implements EnvironmentChangeListener {

    /**
     * Initial (default) packet size for TDS packets.
     */
    public static final int INITIAL_PACKET_SIZE = 8000;

    private CompositeByteBuf lastChunkRemainder;

    private final PacketIdProvider packetIdProvider;

    private int packetSize;

    private HeaderOptions headerOptions;

    /**
     * Creates a new {@link TdsEncoder} using the default {@link #INITIAL_PACKET_SIZE packet size.}.
     *
     * @param packetIdProvider provider for the {@literal packetId}.
     */
    public TdsEncoder(PacketIdProvider packetIdProvider) {
        this(packetIdProvider, INITIAL_PACKET_SIZE);
    }

    /**
     * Creates a new {@link TdsEncoder} using the given {@code packetSize}
     *
     * @param packetIdProvider provider for the {@literal packetId}.
     * @throws IllegalArgumentException when {@link PacketIdProvider} is {@code null}.
     */
    public TdsEncoder(PacketIdProvider packetIdProvider, int packetSize) {

        this.packetIdProvider = Assert.requireNonNull(packetIdProvider, "PacketId Provider must not be null");
        this.packetSize = packetSize;
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

            if (this.headerOptions == null) {
                ctx.write(msg, promise);
                return;
            }

            ByteBuf message = (ByteBuf) msg;

            if (message.readableBytes() <= 0) {
                ctx.write(msg, promise);
                return;
            }

            doWriteFragment(ctx, promise, message, this.headerOptions, true);
            return;
        }

        // Write entire TDSPacket
        if (msg instanceof TdsPacket) {

            TdsPacket packet = (TdsPacket) msg;
            ByteBuf message = packet.encode(ctx.alloc(), this.packetIdProvider);

            Assert.state(message.readableBytes() <= this.packetSize, "Packet size exceeded");

            ctx.write(message, promise);
            return;
        }

        // Write message use HeaderOptions for subsequent packets and apply HeaderOptions
        if (msg instanceof FirstTdsFragment) {

            FirstTdsFragment fragment = (FirstTdsFragment) msg;

            this.headerOptions = fragment.getHeaderOptions();

            doWriteFragment(ctx, promise, fragment.getByteBuf(), this.headerOptions, false);
            return;
        }

        // Write message use HeaderOptions for subsequent packets and apply HeaderOptions
        if (msg instanceof ContextualTdsFragment) {

            ContextualTdsFragment fragment = (ContextualTdsFragment) msg;

            doWriteFragment(ctx, promise, fragment.getByteBuf(), fragment.getHeaderOptions(), true);
            return;
        }

        // Write message, apply HeaderOptions and clear HeaderOptions
        if (msg instanceof LastTdsFragment) {

            Assert.state(this.headerOptions != null, "HeaderOptions must not be null!");

            TdsFragment fragment = (TdsFragment) msg;

            doWriteFragment(ctx, promise, fragment.getByteBuf(), this.headerOptions, true);

            this.headerOptions = null;
            return;
        }

        // Write message and apply HeaderOptions
        if (msg instanceof TdsFragment) {

            Assert.state(this.headerOptions != null, "HeaderOptions must not be null!");

            TdsFragment fragment = (TdsFragment) msg;

            doWriteFragment(ctx, promise, fragment.getByteBuf(), this.headerOptions, false);
            return;
        }

        throw new IllegalArgumentException(String.format("Unsupported message type: %s", msg));
    }

    @Override
    public void onEnvironmentChange(EnvironmentChangeEvent event) {

        EnvChangeToken token = event.getToken();
        if (token.getChangeType() == EnvChangeToken.EnvChangeType.Packetsize) {
            setPacketSize(Integer.parseInt(token.getNewValueString()));
        }
    }

    public void setPacketSize(int packetSize) {
        this.packetSize = packetSize;
    }

    public int getPacketSize() {
        return this.packetSize;
    }

    private static HeaderOptions getLastHeader(HeaderOptions headerOptions) {
        return headerOptions.and(Status.StatusBit.EOM);
    }

    private static HeaderOptions getChunkedHeaderOptions(HeaderOptions headerOptions) {
        return headerOptions.not(Status.StatusBit.EOM);
    }

    private void doWriteFragment(ChannelHandlerContext ctx, ChannelPromise promise, ByteBuf body,
                                 HeaderOptions headerOptions, boolean lastLogicalPacket) {

        if (requiresChunking(body.readableBytes())) {
            writeChunkedMessage(ctx, promise, body, headerOptions, lastLogicalPacket);
        } else {
            writeSingleMessage(ctx, promise, body, headerOptions, lastLogicalPacket);
        }

        body.release();
    }

    private void writeSingleMessage(ChannelHandlerContext ctx, ChannelPromise promise, ByteBuf body, HeaderOptions headerOptions, boolean lastLogicalPacket) {

        if (lastLogicalPacket || getBytesToWrite(body.readableBytes()) == getPacketSize()) {

            HeaderOptions optionsToUse = lastLogicalPacket ? getLastHeader(headerOptions) : headerOptions;

            int messageLength = getBytesToWrite(body.readableBytes());
            ByteBuf buffer = ctx.alloc().buffer(messageLength);
            Header.encode(buffer, optionsToUse, messageLength, this.packetIdProvider);

            if (this.lastChunkRemainder != null) {

                buffer.writeBytes(this.lastChunkRemainder);

                this.lastChunkRemainder.release();
                this.lastChunkRemainder = null;
            }

            buffer.writeBytes(body);

            ctx.write(buffer, promise);
        } else {

            // Prevent partial packets/buffer underrun if not the last packet.
            if (this.lastChunkRemainder == null) {
                this.lastChunkRemainder = body.alloc().compositeBuffer();
            }

            this.lastChunkRemainder.addComponent(true, body.retain());

            ctx.write(Unpooled.EMPTY_BUFFER, promise);
        }
    }

    private void writeChunkedMessage(ChannelHandlerContext ctx, ChannelPromise promise, ByteBuf body, HeaderOptions headerOptions, boolean lastLogicalPacket) {

        PromiseCombiner combiner = new PromiseCombiner(ctx.executor());

        try {
            while (body.readableBytes() > 0) {

                ByteBuf chunk = body.alloc().buffer(estimateChunkSize(getBytesToWrite(body.readableBytes())));

                if (this.lastChunkRemainder != null) {

                    int combinedSize = this.lastChunkRemainder.readableBytes() + body.readableBytes();
                    HeaderOptions optionsToUse = isLastTransportPacket(combinedSize, lastLogicalPacket) ? getLastHeader(headerOptions) : getChunkedHeaderOptions(headerOptions);
                    Header.encode(chunk, optionsToUse, this.packetSize, this.packetIdProvider);

                    int actualBodyReadableBytes = this.packetSize - Header.LENGTH - this.lastChunkRemainder.readableBytes();
                    chunk.writeBytes(this.lastChunkRemainder);
                    chunk.writeBytes(body, actualBodyReadableBytes);

                    this.lastChunkRemainder.release();
                    this.lastChunkRemainder = null;

                } else {

                    if (!lastLogicalPacket && !requiresChunking(body.readableBytes())) {

                        // Prevent partial packets/buffer underrun if not the last packet.
                        this.lastChunkRemainder = body.alloc().compositeBuffer();
                        this.lastChunkRemainder.addComponent(true, body.retain());
                        break;
                    }

                    HeaderOptions optionsToUse = isLastTransportPacket(body.readableBytes(), lastLogicalPacket) ? getLastHeader(headerOptions) : getChunkedHeaderOptions(headerOptions);

                    int byteCount = getEffectiveChunkSizeWithoutHeader(body.readableBytes());
                    Header.encode(chunk, optionsToUse, Header.LENGTH + byteCount, this.packetIdProvider);

                    chunk.writeBytes(body, byteCount);
                }

                combiner.add(ctx.write(chunk, ctx.newPromise()));
            }

            combiner.finish(promise);
        } catch (RuntimeException e) {
            promise.tryFailure(e);
            throw e;
        }
    }

    int estimateChunkSize(int readableBytes) {
        return Math.min(readableBytes + Header.LENGTH, this.packetSize);
    }

    private boolean requiresChunking(int readableBytes) {
        return getBytesToWrite(readableBytes) > this.packetSize;
    }

    private int getBytesToWrite(int readableBytes) {
        int bytesToWrite = Header.LENGTH;
        bytesToWrite += this.lastChunkRemainder != null ? this.lastChunkRemainder.readableBytes() : 0;
        bytesToWrite += readableBytes;
        return bytesToWrite;
    }

    private int getEffectiveChunkSizeWithoutHeader(int readableBytes) {
        return Math.min(readableBytes, this.packetSize - Header.LENGTH);
    }

    private boolean isLastTransportPacket(int readableBytes, boolean lastLogicalPacket) {

        if (requiresChunking(readableBytes)) {
            return false;
        }

        return lastLogicalPacket;
    }

    /**
     * Marker message to reset {@link HeaderOptions}.
     */
    public enum ResetHeader {
        INSTANCE;
    }

}
