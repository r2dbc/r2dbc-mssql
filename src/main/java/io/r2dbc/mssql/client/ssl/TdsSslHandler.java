/*
 * Copyright 2018 the original author or authors.
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

package io.r2dbc.mssql.client.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
import io.r2dbc.mssql.client.ConnectionState;
import io.r2dbc.mssql.client.TdsEncoder;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.ContextualTdsFragment;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * SSL handling for TDS connections.
 * <p/>
 * This handler wraps or passes thru read and write data depending on the {@link SslState}. Because TDS requires header
 * wrapping, we're not mounting the {@link SslHandler} directly into the pipeline but delegating read and write events
 * to it.<p/>
 * This {@link ChannelHandler} supports also full SSL mode and requires to be reordered once the handshake is done therefor it's marked as {@code @Sharable}.
 *
 * @author Mark Paluch
 * @see SslHandler
 * @see ConnectionState
 */
@ChannelHandler.Sharable
public final class TdsSslHandler extends ChannelDuplexHandler {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private volatile SslHandler sslHandler;

    private final PacketIdProvider packetIdProvider;

    private ChannelHandlerContext context;

    private ByteBuf outputBuffer;

    private SslState state = SslState.OFF;

    private boolean handshakeDone;

    @Nullable
    private Chunk chunk;

    /**
     * Creates a new {@link TdsSslHandler}.
     *
     * @param packetIdProvider the {@link PacketIdProvider} to create {@link Header}s to wrap the SSL handshake.
     */
    public TdsSslHandler(PacketIdProvider packetIdProvider) {

        Assert.requireNonNull(packetIdProvider, "PacketIdProvider must not be null");

        this.packetIdProvider = packetIdProvider;
    }

    /**
     * Create the {@link SslHandler}.
     *
     * @return
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    private SslHandler createSslHandler() throws NoSuchAlgorithmException, KeyManagementException {

        SSLContext sslContext = SSLContext.getInstance("TLS");
        TrustManager tms[] = new TrustManager[]{new X509TrustManager() {

            @Override
            public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

            }

            @Override
            public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }};

        sslContext.init(null, tms, null);

        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);
        return new SslHandler(sslEngine);
    }

    /**
     * Lazily register {@link SslHandler} if needed.
     *
     * @param ctx
     * @param evt
     * @throws Exception
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt == SslState.LOGIN_ONLY || evt == SslState.CONNECTION) {

            this.state = (SslState) evt;
            this.sslHandler = createSslHandler();

            logger.debug("Registering Context Proxy and SSL Event Handlers to propagate SSL events to channelRead()");
            ctx.pipeline().addAfter(getClass().getName(), ContextProxy.class.getName(), new ContextProxy());
            ctx.pipeline().addAfter(ContextProxy.class.getName(), SslEventHandler.class.getName(), new SslEventHandler());

            this.context = ctx.channel().pipeline().context(ContextProxy.class.getName());

            ctx.write(HeaderOptions.create(Type.PRE_LOGIN, Status.empty()));

            this.sslHandler.handlerAdded(this.context);
        }

        if (evt == SslState.NEGOTIATED) {

            logger.debug("SSL Handshake done");

            ctx.write(TdsEncoder.ResetHeader.INSTANCE, ctx.voidPromise());
            this.handshakeDone = true;

            // Reorder handlers:
            // 1. Apply TLS first
            // 2. Logging next
            // 3. TDS
            if (this.state == SslState.CONNECTION) {

                logger.debug("Reordering handlers for full SSL usage");

                ctx.pipeline().remove(this);
                ctx.pipeline().addFirst(this);
            }
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.outputBuffer = ctx.alloc().buffer();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {

        if (this.outputBuffer != null) {
            this.outputBuffer.release();
            this.outputBuffer = null;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        if (this.sslHandler != null) {
            this.sslHandler.channelInactive(ctx);
        }

        Chunk chunk = this.chunk;
        if (chunk != null) {
            chunk.buffer.release();
            this.chunk = null;
        }
    }

    /**
     * Write data either directly (SSL disabled), to an intermediate buffer for {@link Header} wrapping, or via
     * {@link SslHandler} for entire packet encryption without prepending a header. Note that {@link SslState#LOGIN_ONLY}
     * is swapped to {@link SslState#AFTER_LOGIN_ONLY} once login payload is written. We don't check actually whether the
     * payload is a login packet but rely on higher level layers to send the appropriate data.
     *
     * @param ctx
     * @param msg
     * @param promise
     * @throws Exception
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if (this.handshakeDone && (this.state == SslState.NEGOTIATED || this.state == SslState.LOGIN_ONLY || this.state == SslState.CONNECTION)) {

            msg = unwrap(ctx.alloc(), msg);

            this.sslHandler.write(ctx, msg, promise);
            this.sslHandler.flush(ctx);

            if (this.state == SslState.LOGIN_ONLY) {
                this.state = SslState.AFTER_LOGIN_ONLY;
            }

            return;
        }

        if (requiresWrapping()) {

            logger.debug("Write wrapping: Append to output buffer");

            ByteBuf sslPayload = (ByteBuf) msg;

            this.outputBuffer.writeBytes(sslPayload);

            sslPayload.release();
        } else {
            super.write(ctx, msg, promise);
        }
    }

    private Object unwrap(ByteBufAllocator allocator, Object msg) {

        if (msg instanceof ContextualTdsFragment) {

            ContextualTdsFragment tdsFragment = (ContextualTdsFragment) msg;

            HeaderOptions headerOptions = tdsFragment.getHeaderOptions();
            Status eom = headerOptions.getStatus().and(Status.StatusBit.EOM);
            Header header = new Header(headerOptions.getType(), eom, Header.LENGTH + tdsFragment.getByteBuf().readableBytes(),
                0, this.packetIdProvider.nextPacketId(), 0);

            ByteBuf buffer = allocator.buffer(header.getLength());
            header.encode(buffer);
            buffer.writeBytes(tdsFragment.getByteBuf());
            tdsFragment.getByteBuf().release();

            // unwrap ByteBuffer so we can write it using SSL.
            return buffer;
        }

        if (msg instanceof TdsFragment) {
            // unwrap ByteBuffer so we can write it using SSL.
            return ((TdsFragment) msg).getByteBuf();
        }

        return msg;
    }

    /**
     * Wrap SSL handshake in prelogin {@link Header}s. Delaying write to flush instead of writing each packet in a single
     * header.
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {

        if (requiresWrapping()) {

            logger.debug("Write wrapping: Flushing output buffer and enable auto-read");

            ByteBuf message = this.outputBuffer;
            this.outputBuffer = ctx.alloc().buffer();

            ctx.writeAndFlush(message);
            ctx.channel().config().setAutoRead(true);
        } else {
            super.flush(ctx);
        }
    }

    /**
     * SSL quirk: Make sure to flush the output buffer during SSL handshake. The SSL handshake can end in the read
     * underrun that wants to read from the transport. We need to make sure that write requests (that don't get flushed
     * explicitly) are sent so we can expect eventually a read.
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {

        if (isInHandshake() && this.outputBuffer.readableBytes() > 0) {
            flush(ctx);
        }

        super.channelReadComplete(ctx);
    }

    /**
     * Route read events to the {@link SslHandler}. Strip off {@link Header} during handshake.
     *
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (isInHandshake()) {
            ByteBuf buffer = (ByteBuf) msg;

            Chunk chunk = this.chunk;
            if (chunk != null || Header.canDecode(buffer)) {

                Header header;
                if (chunk == null) {

                    header = Header.decode(buffer);

                    // sub-chunk read
                    if (!Chunk.isCompletePacketAvailable(header, buffer)) {
                        this.chunk = new Chunk(header, buffer);
                        ctx.read();
                        return;
                    }
                } else {

                    chunk.buffer.writeBytes(buffer);
                    buffer.release();

                    if (!chunk.isCompletePacketAvailable()) {
                        return;
                    }

                    buffer = chunk.buffer;
                    header = chunk.header;
                    this.chunk = null;
                }

                if (header.getType() == Type.PRE_LOGIN) {
                    this.sslHandler.channelRead(this.context, buffer);
                }

                if (header.is(Status.StatusBit.IGNORE)) {
                    return;
                }
            }
            return;
        }

        if (handshakeDone && this.state == SslState.CONNECTION) {
            this.sslHandler.channelRead(ctx, msg);
            return;
        }

        super.channelRead(ctx, msg);
    }

    private boolean isInHandshake() {
        return requiresWrapping() && !this.handshakeDone;
    }

    private boolean requiresWrapping() {
        return (this.state == SslState.LOGIN_ONLY || this.state == SslState.CONNECTION);
    }

    /**
     * Chunk remainder for incomplete packet reads.
     */
    static class Chunk {

        final Header header;

        final ByteBuf buffer;

        Chunk(Header header, ByteBuf buffer) {
            this.header = header;
            this.buffer = buffer;
        }

        boolean isCompletePacketAvailable() {
            return isCompletePacketAvailable(this.header, this.buffer);
        }

        /**
         * Check if the full packet arrived. Since we've already read the header, we need to add {@link Header#LENGTH} to the calculation.
         *
         * @param header
         * @param buffer
         * @return
         */
        static boolean isCompletePacketAvailable(Header header, ByteBuf buffer) {
            return (buffer.readableBytes() + Header.LENGTH) >= header.getLength();
        }
    }
}
