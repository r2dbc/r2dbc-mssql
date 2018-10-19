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

package io.r2dbc.mssql.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.PacketIdProvider;

import org.reactivestreams.Publisher;

/**
 * A message sent from a frontend client to a backend server.
 */
public interface ClientMessage extends Message {

	/**
	 * Encode a message into a {@link ByteBuf}.
	 *
	 * @param allocator the {@link ByteBufAllocator} to use to get a {@link ByteBuf} to write into
	 * @param packetIdProvider the {@link PacketIdProvider} to generate the packet id header
	 * @return a {@link Publisher} that produces the {@link ByteBuf} containing the encoded message
	 */
	Publisher<ByteBuf> encode(ByteBufAllocator allocator, PacketIdProvider packetIdProvider);

}
