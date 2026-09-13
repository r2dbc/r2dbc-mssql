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
package io.r2dbc.mssql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for SQL Server named-instance discovery.
 *
 * <p>The SQL Server itself is provided by the existing Testcontainers-based
 * {@link IntegrationTestSupport}. SQL Server on Linux does not provide named
 * instances or SQL Server Browser, so this test supplies only the SSRP endpoint
 * with an in-process UDP server. The SSRP response points to the mapped TCP port
 * of the real SQL Server Testcontainer.
 *
 * @author Daniel Pachali
 */
class NamedInstanceIntegrationTests extends IntegrationTestSupport {

    private static final int SQL_BROWSER_PORT = 1434;

    private static final String INSTANCE_NAME = "SQLEXPRESS";

    private static final Duration SERVER_START_TIMEOUT = Duration.ofSeconds(5);

    private Connection browserServer;

    @AfterEach
    void stopBrowserServer() {

        if (this.browserServer != null) {
            this.browserServer.disposeNow(Duration.ofSeconds(5));
        }
    }

    @Test
    void shouldResolveNamedInstanceAndConnectToSqlServer() {

        AtomicReference<byte[]> receivedRequest = new AtomicReference<>();

        startBrowserServer(receivedRequest);

        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, MssqlConnectionFactoryProvider.MSSQL_DRIVER)
            .option(HOST, SERVER.getHost())
            .option(DATABASE, "master")
            .option(USER, SERVER.getUsername())
            .option(PASSWORD, SERVER.getPassword())
            .option(MssqlConnectionFactoryProvider.INSTANCE_NAME, INSTANCE_NAME)
            .build();

        MssqlConnectionFactory connectionFactory =
            (MssqlConnectionFactory) ConnectionFactories.get(options);

        Flux.usingWhen(
                connectionFactory.create(),
                connection -> connection.createStatement("SELECT 1 AS value")
                    .execute()
                    .flatMap(result -> result.map((row, metadata) ->
                        row.get("value", Integer.class))),
                MssqlConnection::close)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        assertThat(receivedRequest.get()).isNotNull();
        assertThat(ByteBufUtil.hexDump(receivedRequest.get()))
            .isEqualToIgnoringCase("0453514c4558505245535300");
    }

    private void startBrowserServer(AtomicReference<byte[]> receivedRequest) {

        this.browserServer = UdpServer.create()
            .host(SERVER.getHost())
            .port(SQL_BROWSER_PORT)
            .handle((in, out) -> out.sendObject(
                in.receiveObject()
                    .ofType(DatagramPacket.class)
                    .map(packet -> {
                        receivedRequest.set(ByteBufUtil.getBytes(packet.content()));

                        return new DatagramPacket(
                            browserResponse(
                                "ServerName;TESTCONTAINER;" +
                                    "InstanceName;" + INSTANCE_NAME + ";" +
                                    "IsClustered;No;" +
                                    "Version;16.0.1000.6;" +
                                    "tcp;" + SERVER.getPort() + ";;"),
                            packet.sender());
                    })))
            .bindNow(SERVER_START_TIMEOUT);
    }

    private static ByteBuf browserResponse(String responseData) {

        byte[] bytes = responseData.getBytes(StandardCharsets.US_ASCII);

        ByteBuf buffer = Unpooled.buffer(3 + bytes.length);

        buffer.writeByte(0x05);
        buffer.writeShortLE(bytes.length);
        buffer.writeBytes(bytes);

        return buffer;
    }
}
