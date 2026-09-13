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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import io.r2dbc.mssql.message.tds.ProtocolException;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;
import reactor.test.StepVerifier;

/**
 * Unit tests for {@link SqlServerBrowserClient}.
 *
 * @author Daniel Pachali
 */
final class SqlServerBrowserClientUnitTests {

    private static final String LOOPBACK_ADDRESS = "127.0.0.1";

    private static final Duration SERVER_START_TIMEOUT = Duration.ofSeconds(5);

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);

    private Connection server;

    @AfterEach
    void tearDown() {

        if (this.server != null) {
            this.server.disposeNow(Duration.ofSeconds(5));
        }
    }

    @Test
    void shouldResolveTcpPortAndSendNamedInstanceRequest() {

        AtomicReference<byte[]> receivedRequest = new AtomicReference<>();

        startRespondingServer(packet -> {

            receivedRequest.set(
                    ByteBufUtil.getBytes(packet.content()));

            return browserResponse(
                    "ServerName;SQL01;" +
                            "InstanceName;SQLEXPRESS;" +
                            "IsClustered;No;" +
                            "Version;16.0.1000.6;" +
                            "tcp;49731;;");
        });

        SqlServerBrowserClient client = new SqlServerBrowserClient(serverPort(), TEST_TIMEOUT);

        StepVerifier.create(
                client.resolvePort(LOOPBACK_ADDRESS, "SQLEXPRESS"))
                .expectNext(49731)
                .expectComplete()
                .verify(Duration.ofSeconds(5));

        assertThat(receivedRequest.get()).isNotNull();

        assertThat(ByteBufUtil.hexDump(receivedRequest.get()))
                .isEqualToIgnoringCase(
                        "0453514c4558505245535300");
    }

    @Test
    void shouldPropagateInvalidBrowserResponse() {

        startRespondingServer(packet -> browserResponse(
                "ServerName;SQL01;" +
                        "InstanceName;SQLEXPRESS;" +
                        "IsClustered;No;" +
                        "Version;16.0.1000.6;;"));

        SqlServerBrowserClient client = new SqlServerBrowserClient(serverPort(), TEST_TIMEOUT);

        StepVerifier.create(
                client.resolvePort(LOOPBACK_ADDRESS, "SQLEXPRESS"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ProtocolException.class)
                        .hasMessageContaining(
                                "does not contain TCP protocol information"))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void shouldTimeoutWhenBrowserDoesNotRespond() {

        startSilentServer();

        SqlServerBrowserClient client = new SqlServerBrowserClient(
                serverPort(),
                Duration.ofMillis(500));

        StepVerifier.create(
                client.resolvePort(LOOPBACK_ADDRESS, "SQLEXPRESS"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining(
                                "SQL Server Browser did not respond")
                        .hasMessageContaining("SQLEXPRESS"))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void shouldRejectInvalidArguments() {

        SqlServerBrowserClient client = new SqlServerBrowserClient(
                1434,
                Duration.ofSeconds(1));

        assertThatThrownBy(() -> client.resolvePort(null, "SQLEXPRESS"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host must not be null");

        assertThatThrownBy(() -> client.resolvePort("", "SQLEXPRESS"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host must not be empty");

        assertThatThrownBy(() -> client.resolvePort("SQL01", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "instanceName must not be null");

        assertThatThrownBy(() -> client.resolvePort("SQL01", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "instanceName must not be empty");
    }

    @Test
    void shouldRejectInvalidConfiguration() {

        assertThatThrownBy(() -> new SqlServerBrowserClient(
                0,
                Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "browserPort must be between 1 and 65535");

        assertThatThrownBy(() -> new SqlServerBrowserClient(
                65536,
                Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "browserPort must be between 1 and 65535");

        assertThatThrownBy(() -> new SqlServerBrowserClient(1434, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout must not be null");

        assertThatThrownBy(() -> new SqlServerBrowserClient(
                1434,
                Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "timeout must be greater than zero");
    }

    private void startRespondingServer(
            Function<DatagramPacket, ByteBuf> responseFactory) {

        this.server = UdpServer.create()
                .host(LOOPBACK_ADDRESS)
                .port(0)
                .handle((in, out) -> out.sendObject(
                        in.receiveObject()
                                .ofType(DatagramPacket.class)
                                .map(packet -> new DatagramPacket(
                                        responseFactory.apply(packet),
                                        packet.sender()))))
                .bindNow(SERVER_START_TIMEOUT);
    }

    private void startSilentServer() {

        this.server = UdpServer.create()
                .host(LOOPBACK_ADDRESS)
                .port(0)
                .handle((in, out) -> in.receive().then())
                .bindNow(SERVER_START_TIMEOUT);
    }

    private int serverPort() {

        return ((InetSocketAddress) this.server.address())
                .getPort();
    }

    private static ByteBuf browserResponse(
            String responseData) {

        byte[] bytes = responseData.getBytes(StandardCharsets.US_ASCII);

        ByteBuf buffer = Unpooled.buffer(3 + bytes.length);

        buffer.writeByte(0x05);
        buffer.writeShortLE(bytes.length);
        buffer.writeBytes(bytes);

        return buffer;
    }

}