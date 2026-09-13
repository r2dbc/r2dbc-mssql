/*
 * Copyright 2026 the original author or authors.
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
import io.r2dbc.mssql.util.Assert;

import java.nio.charset.Charset;

/**
 * Encoder for SQL Server Browser requests.
 *
 * @author Daniel Pachali
 */
final class SqlServerBrowserRequestEncoder {

    private static final int CLNT_UCAST_INST = 0x04;

    private static final int MAX_INSTANCE_NAME_LENGTH = 32;

    private SqlServerBrowserRequestEncoder() {
    }

    /**
     * Encode a SQL Server Browser {@code CLNT_UCAST_INST} request.
     *
     * @param buffer       the target {@link ByteBuf}.
     * @param instanceName the SQL Server instance name.
     * @param charset      the character set used to encode the instance name.
     */
    static void encode(ByteBuf buffer, String instanceName, Charset charset) {

        Assert.requireNonNull(buffer, "buffer must not be null");
        Assert.requireNonNull(instanceName, "instanceName must not be null");
        Assert.requireNonNull(charset, "charset must not be null");

        Assert.isTrue(!instanceName.isEmpty(),
            "instanceName must not be empty");

        Assert.isTrue(instanceName.indexOf('\0') == -1,
            "instanceName must not contain a null character");

        Assert.isTrue(charset.newEncoder().canEncode(instanceName),
            String.format("instanceName cannot be encoded using charset %s", charset.name()));

        byte[] instanceNameBytes = instanceName.getBytes(charset);

        Assert.isTrue(instanceNameBytes.length <= MAX_INSTANCE_NAME_LENGTH,
            String.format("instanceName must not exceed %d bytes", MAX_INSTANCE_NAME_LENGTH));

        buffer.ensureWritable(instanceNameBytes.length + 2);

        buffer.writeByte(CLNT_UCAST_INST);
        buffer.writeBytes(instanceNameBytes);
        buffer.writeByte(0);
    }

}