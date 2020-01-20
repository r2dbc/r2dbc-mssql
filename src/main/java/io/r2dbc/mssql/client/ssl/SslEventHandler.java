/*
 * Copyright 2018-2020 the original author or authors.
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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

/**
 * Event handler for SSL negotiation events.
 *
 * @author Mark Paluch
 */
class SslEventHandler extends ChannelDuplexHandler {

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt == SslHandshakeCompletionEvent.SUCCESS) {
            ctx.channel().pipeline().fireUserEventTriggered(SslState.NEGOTIATED);
        }

        if (evt == SslState.NEGOTIATED) {
            ctx.fireChannelRead(SslState.NEGOTIATED);
        }

        super.userEventTriggered(ctx, evt);
    }
}
