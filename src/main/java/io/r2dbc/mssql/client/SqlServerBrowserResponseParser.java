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
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.util.Assert;

import java.nio.charset.StandardCharsets;

/**
 * Parser for SQL Server Browser responses.
 *
 * @author Daniel Pachali
 */
final class SqlServerBrowserResponseParser {

    private static final int SVR_RESP = 0x05;

    private static final int MAX_INSTANCE_RESPONSE_LENGTH = 1024;

    private SqlServerBrowserResponseParser() {
    }

    /**
     * Parse the TCP port from a SQL Server Browser {@code SVR_RESP}.
     *
     * @param buffer the response buffer.
     * @return the resolved TCP port.
     */
    static int parseTcpPort(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "buffer must not be null");

        if (buffer.readableBytes() < 3) {
            throw new ProtocolException("SQL Server Browser response is too short");
        }

        int responseType = buffer.readUnsignedByte();

        if (responseType != SVR_RESP) {
            throw new ProtocolException(String.format(
                "Unexpected SQL Server Browser response type: 0x%02X", responseType));
        }

        int responseLength = buffer.readUnsignedShortLE();

        if (responseLength > MAX_INSTANCE_RESPONSE_LENGTH) {
            throw new ProtocolException(String.format(
                "SQL Server Browser response exceeds maximum length of %d bytes",
                MAX_INSTANCE_RESPONSE_LENGTH));
        }

        if (buffer.readableBytes() < responseLength) {
            throw new ProtocolException(String.format(
                "Incomplete SQL Server Browser response: expected %d bytes but received %d",
                responseLength, buffer.readableBytes()));
        }

        String response = buffer.readCharSequence(
            responseLength, StandardCharsets.US_ASCII).toString();

        String[] tokens = response.split(";", -1);

        for (int i = 0; i + 1 < tokens.length; i += 2) {

            if (!"tcp".equalsIgnoreCase(tokens[i])) {
                continue;
            }

            String portValue = tokens[i + 1];

            try {

                int port = Integer.parseInt(portValue);

                if (port < 1 || port > 65535) {
                    throw new ProtocolException(String.format(
                        "Invalid TCP port in SQL Server Browser response: %s",
                        portValue));
                }

                return port;

            } catch (NumberFormatException e) {
                throw new ProtocolException(String.format(
                    "Invalid TCP port in SQL Server Browser response: %s",
                    portValue), e);
            }
        }

        throw new ProtocolException(
            "SQL Server Browser response does not contain TCP protocol information");
    }

}