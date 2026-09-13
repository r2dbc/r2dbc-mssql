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

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.Assert;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.udp.UdpClient;

/**
 * Client for resolving named SQL Server instances through SQL Server Browser.
 *
 * @author Daniel Pachali
 */
public final class SqlServerBrowserClient {

    private static final int DEFAULT_BROWSER_PORT = 1434;

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private final int browserPort;

    private final Duration timeout;

    /**
     * Create a SQL Server Browser client using UDP port {@code 1434}.
     */
    public SqlServerBrowserClient() {
        this(DEFAULT_BROWSER_PORT, DEFAULT_TIMEOUT);
    }

    /**
     * Create a SQL Server Browser client.
     *
     * @param browserPort the SQL Server Browser UDP port.
     * @param timeout     the timeout for the browser request.
     */
    SqlServerBrowserClient(int browserPort, Duration timeout) {

        Assert.isTrue(browserPort > 0 && browserPort <= 65535,
                "browserPort must be between 1 and 65535");

        this.timeout = Assert.requireNonNull(timeout,
                "timeout must not be null");

        Assert.isTrue(timeout.compareTo(Duration.ZERO) > 0,
                "timeout must be greater than zero");

        this.browserPort = browserPort;
    }

    /**
     * Resolve the TCP port for a named SQL Server instance.
     *
     * @param host         the SQL Server host.
     * @param instanceName the SQL Server instance name.
     * @return a {@link Mono} emitting the resolved TCP port.
     */
    public Mono<Integer> resolvePort(String host, String instanceName) {

        Assert.requireNonNull(host, "host must not be null");
        Assert.requireNonNull(instanceName, "instanceName must not be null");

        Assert.isTrue(!host.isEmpty(), "host must not be empty");
        Assert.isTrue(!instanceName.isEmpty(),
                "instanceName must not be empty");

        return Mono.defer(() -> {

            Sinks.One<Integer> result = Sinks.one();

            UdpClient client = UdpClient.create()
                    .host(host)
                    .port(this.browserPort)
                    .handle((in, out) -> {

                        ByteBuf request = out.alloc().buffer();

                        try {
                            SqlServerBrowserRequestEncoder.encode(
                                    request,
                                    instanceName,
                                    StandardCharsets.US_ASCII);
                        } catch (RuntimeException e) {
                            request.release();
                            return Mono.error(e);
                        }

                        return out.send(Mono.just(request))
                                .then(in.receive()
                                        .next()
                                        .map(SqlServerBrowserResponseParser::parseTcpPort)
                                        .doOnNext(port -> result.tryEmitValue(port))
                                        .then())
                                .then()
                                .doOnError(error -> result.tryEmitError(error));
                    });

            Mono<Integer> resolution = client.connect()
                    .flatMap(connection -> result.asMono()
                            .doFinally(signalType -> connection.dispose()));

            return resolution.timeout(
                    this.timeout,
                    Mono.error(new IllegalStateException(String.format(
                            "SQL Server Browser did not respond for instance '%s' on host '%s' within %d ms",
                            instanceName,
                            host,
                            this.timeout.toMillis()))));
        });
    }

}