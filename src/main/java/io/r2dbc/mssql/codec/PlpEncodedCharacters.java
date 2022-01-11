/*
 * Copyright 2019-2022 the original author or authors.
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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.SqlServerType;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;

/**
 * Extension to {@link PlpEncoded} associated with a {@link Collation}.
 *
 * @author Mark Paluch
 */
class PlpEncodedCharacters extends PlpEncoded {

    private final Collation collation;

    PlpEncodedCharacters(SqlServerType dataType, Collation collation, ByteBufAllocator allocator, Publisher<ByteBuf> dataStream, Disposable disposable) {

        super(dataType, allocator, dataStream, disposable);

        this.collation = collation;
    }

    public void encodeHeader(ByteBuf byteBuf) {

        super.encodeHeader(byteBuf);

        this.collation.encode(byteBuf);
    }

}
