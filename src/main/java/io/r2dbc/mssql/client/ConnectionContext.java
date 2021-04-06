/*
 * Copyright 2019-2021 the original author or authors.
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

import io.r2dbc.mssql.MssqlConnectionConfiguration;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Value object capturing diagnostic connection context. Allows for log-message post-processing with {@link #getMessage(String) if the logger category for
 * {@code io.r2dbc.mssql.client.ConnectionContext} is enabled for DEBUG/TRACE logs.
 * <p>
 * Captures also the configured {@link MssqlConnectionConfiguration#getApplicationName()}  application name} and {@link MssqlConnectionConfiguration#getConnectionId() connection Id}.
 *
 * @author Mark Paluch
 */
public class ConnectionContext {

    private final static Logger LOGGER = Loggers.getLogger(ConnectionContext.class.getName() + ".context");

    private final static boolean CONTEXT_ENABLED = LOGGER.isDebugEnabled();

    private final static boolean CHANNEL_ID_ENABLED = LOGGER.isTraceEnabled();

    private static final AtomicLong CONN_ID = new AtomicLong();

    @Nullable
    private final String applicationName;

    @Nullable
    private final UUID connectionId;

    @Nullable
    private final String channelId;

    private final String connectionCounter;

    private final String connectionIdPrefix;

    /**
     * Create a new {@link ConnectionContext} with a unique connection Id.
     */
    public ConnectionContext() {
        this.applicationName = null;
        this.connectionId = null;
        this.connectionCounter = incrementConnectionCounter();
        this.connectionIdPrefix = getConnectionIdPrefix();
        this.channelId = null;
    }

    /**
     * Create a new {@link ConnectionContext} with a unique connection Id.
     */
    public ConnectionContext(@Nullable String applicationName, @Nullable UUID connectionId) {

        this.applicationName = applicationName;
        this.connectionId = connectionId;
        this.connectionCounter = incrementConnectionCounter();
        this.connectionIdPrefix = getConnectionIdPrefix();
        this.channelId = null;
    }

    private ConnectionContext(@Nullable String applicationName, @Nullable UUID connectionId, @Nullable String channelId, String connectionCounter, String connectionIdPrefix) {
        this.applicationName = applicationName;
        this.connectionId = connectionId;
        this.channelId = channelId;
        this.connectionCounter = connectionCounter;
        this.connectionIdPrefix = connectionIdPrefix;
    }

    private String incrementConnectionCounter() {
        return Long.toHexString(CONN_ID.incrementAndGet());
    }

    private String getConnectionIdPrefix() {
        return "[cid: 0x" + this.connectionCounter + "] ";
    }

    /**
     * Process the {@code original} message to inject potentially debug information such as the channel Id or the connection Id.
     *
     * @param original the original message.
     * @return the post-processed log message.
     */
    public String getMessage(String original) {

        if (CHANNEL_ID_ENABLED) {
            return this.connectionIdPrefix + this.channelId + " " + original;
        }

        if (CONTEXT_ENABLED) {
            return this.connectionIdPrefix + original;
        }

        return original;
    }

    /**
     * Create a new {@link ConnectionContext} by associating the {@code channelId}.
     *
     * @param channelId the channel identifier.
     * @return a new {@link ConnectionContext} with all previously set values and the associated {@code channelId}.
     */
    public ConnectionContext withChannelId(String channelId) {
        return new ConnectionContext(this.applicationName, this.connectionId, channelId, this.connectionCounter, this.connectionIdPrefix);
    }

}
