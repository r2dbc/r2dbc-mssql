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

package io.r2dbc.mssql.client.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Objects;

/**
 * SSL handling for TDS connections.
 * <p/>
 * This handler wraps or passes thru read and write data depending on the {@link SslState}. Because TDS requires header
 * wrapping, we're not mounting the {@link SslHandler} directly into the pipeline but delegating read and write events
 * to it.
 *
 * @author Mark Paluch
 * @see SslHandler
 * @see ConnectionState
 */
public final class TdsSslHandler extends ChannelDuplexHandler {

    private volatile SslHandler sslHandler;

    private final PacketIdProvider packetIdProvider;

    private volatile ChannelHandlerContext context;

    private volatile ByteBuf outputBuffer;

    private volatile SslState state = SslState.OFF;

    private volatile boolean handshakeDone;

    /**
     * Creates a new {@link TdsSslHandler}.
     *
     * @param packetIdProvider the {@link PacketIdProvider} to create {@link Header}s to wrap the SSL handshake.
     */
    public TdsSslHandler(PacketIdProvider packetIdProvider) {

        Objects.requireNonNull(packetIdProvider, "PacketIdProvider must not be null");

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

        if (evt == SslState.LOGIN_ONLY) {

            this.state = SslState.LOGIN_ONLY;
            this.sslHandler = createSslHandler();

            ctx.pipeline().addAfter(getClass().getName(), ContextProxy.class.getName(), new ContextProxy());
            ctx.pipeline().addAfter(ContextProxy.class.getName(), SslEventHandler.class.getName(), new SslEventHandler());

            this.context = ctx.channel().pipeline().context(ContextProxy.class.getName());

            ctx.write(HeaderOptions.create(Type.PRE_LOGIN, Status.empty()));

            this.sslHandler.handlerAdded(this.context);
        }

        if (evt == SslState.NEGOTIATED) {
            ctx.write(TdsEncoder.ResetHeader.INSTANCE, ctx.voidPromise());
            this.handshakeDone = true;
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.outputBuffer = ctx.alloc().buffer();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        this.outputBuffer.release();
        this.outputBuffer = null;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.sslHandler.channelInactive(ctx);
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

        if (this.handshakeDone && (this.state == SslState.NEGOTIATED || this.state == SslState.LOGIN_ONLY)) {

            msg = unwrap(ctx.alloc(), msg);

            this.sslHandler.write(ctx, msg, promise);
            this.sslHandler.flush(ctx);

            if (this.state == SslState.LOGIN_ONLY) {
                this.state = SslState.AFTER_LOGIN_ONLY;
            }

            return;
        }

        if (requiresWrapping()) {
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

            ByteBuf message = this.outputBuffer;
            this.outputBuffer = ctx.alloc().buffer();

            ctx.writeAndFlush(message);
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

            if (Header.canDecode(buffer)) {

                buffer.markReaderIndex();

                Header decode = Header.decode(buffer);

                if (decode.getType() == Type.PRE_LOGIN) {
                    this.sslHandler.channelRead(this.context, buffer);
                }

                if (decode.is(Status.StatusBit.IGNORE)) {
                    return;
                }
            }
            return;
        }

        super.channelRead(ctx, msg);
    }

    private boolean isInHandshake() {
        return requiresWrapping() && !this.handshakeDone;
    }

    private boolean requiresWrapping() {
        return this.state == SslState.LOGIN_ONLY;
    }
}
