/*
 * Copyright 2018-2019 the original author or authors.
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
import io.netty.handler.ssl.SslHandler;

/**
 * Empty proxy handler as {@link ChannelHandlerContext} target for {@link SslHandler}. TDS requires wrapping of the SSL
 * handshake during prelogin and we need to wrap/reuse the SSL handler to prepend TDS prelogin headers during the
 * handshake.
 *
 * @author Mark Paluch
 */
class ContextProxy extends ChannelDuplexHandler {

}
