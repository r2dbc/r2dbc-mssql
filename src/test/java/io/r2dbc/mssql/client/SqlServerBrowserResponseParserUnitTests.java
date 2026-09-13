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
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.tds.ProtocolException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SqlServerBrowserResponseParser}.
 *
 * @author Daniel Pachali
 */
final class SqlServerBrowserResponseParserUnitTests {

    @Test
    void shouldParseTcpPort() {

        ByteBuf response = response(
            "ServerName;SQL01;" +
                "InstanceName;SQLEXPRESS;" +
                "IsClustered;No;" +
                "Version;16.0.1000.6;" +
                "tcp;49731;;");

        assertThat(SqlServerBrowserResponseParser.parseTcpPort(response))
            .isEqualTo(49731);
    }

    @Test
    void shouldParseTcpPortRegardlessOfProtocolOrderAndCase() {

        ByteBuf response = response(
            "ServerName;SQL01;" +
                "InstanceName;SQLEXPRESS;" +
                "IsClustered;No;" +
                "Version;16.0.1000.6;" +
                "np;\\\\SQL01\\pipe\\MSSQL$SQLEXPRESS\\sql\\query;" +
                "TCP;51432;;");

        assertThat(SqlServerBrowserResponseParser.parseTcpPort(response))
            .isEqualTo(51432);
    }

    @Test
    void shouldRejectUnexpectedResponseType() {

        ByteBuf response = response(
            "ServerName;SQL01;" +
                "InstanceName;SQLEXPRESS;" +
                "IsClustered;No;" +
                "Version;16.0.1000.6;" +
                "tcp;49731;;");

        response.setByte(0, 0x04);

        assertThatThrownBy(() ->
            SqlServerBrowserResponseParser.parseTcpPort(response))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining(
                "Unexpected SQL Server Browser response type");
    }

    @Test
    void shouldRejectIncompleteResponse() {

        ByteBuf response = response(
            "ServerName;SQL01;" +
                "InstanceName;SQLEXPRESS;" +
                "IsClustered;No;" +
                "Version;16.0.1000.6;" +
                "tcp;49731;;");

        response.writerIndex(response.writerIndex() - 2);

        assertThatThrownBy(() ->
            SqlServerBrowserResponseParser.parseTcpPort(response))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining(
                "Incomplete SQL Server Browser response");
    }

    @Test
    void shouldRejectMissingTcpProtocol() {

        ByteBuf response = response(
            "ServerName;SQL01;" +
                "InstanceName;SQLEXPRESS;" +
                "IsClustered;No;" +
                "Version;16.0.1000.6;" +
                "np;\\\\SQL01\\pipe\\MSSQL$SQLEXPRESS\\sql\\query;;");

        assertThatThrownBy(() ->
            SqlServerBrowserResponseParser.parseTcpPort(response))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining(
                "does not contain TCP protocol information");
    }

    @Test
    void shouldRejectInvalidTcpPort() {

        ByteBuf response = response(
            "ServerName;SQL01;" +
                "InstanceName;SQLEXPRESS;" +
                "IsClustered;No;" +
                "Version;16.0.1000.6;" +
                "tcp;invalid;;");

        assertThatThrownBy(() ->
            SqlServerBrowserResponseParser.parseTcpPort(response))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining(
                "Invalid TCP port in SQL Server Browser response");
    }

    @Test
    void shouldRejectTcpPortOutsideValidRange() {

        ByteBuf response = response(
            "ServerName;SQL01;" +
                "InstanceName;SQLEXPRESS;" +
                "IsClustered;No;" +
                "Version;16.0.1000.6;" +
                "tcp;65536;;");

        assertThatThrownBy(() ->
            SqlServerBrowserResponseParser.parseTcpPort(response))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining(
                "Invalid TCP port in SQL Server Browser response");
    }

    private static ByteBuf response(String responseData) {

        byte[] bytes = responseData.getBytes(StandardCharsets.US_ASCII);

        ByteBuf buffer = Unpooled.buffer(3 + bytes.length);

        buffer.writeByte(0x05);
        buffer.writeShortLE(bytes.length);
        buffer.writeBytes(bytes);

        return buffer;
    }

}