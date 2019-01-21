/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.mssql;

import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.AbstractDoneToken;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility to generated the generated values clause using {@code SCOPE_IDENTITY()}.
 *
 * @author Mark Paluch
 */
final class GeneratedValues {

    private static final String GENERATED_KEYS_QUERY = "SELECT SCOPE_IDENTITY() AS ";

    private static final String DEFAULT_GENERATED_KEY_NAME = "GENERATED_KEYS";

    /**
     * Adjust the message flow to emit the first received count frame at a later time, when a second {@link AbstractDoneToken} is emitted. This reordering prevents multi-result creation (i.e.
     * emitting only a single {@link Result}) and merges the count into the {@link Result} by replacing the second {@link AbstractDoneToken}.
     *
     * @param messages the message flow.
     * @return the transformed message flow.
     */
    static Flux<Message> reduceToSingleCountDoneToken(Flux<? extends Message> messages) {

        return Flux.defer(() -> {

            AtomicReference<AbstractDoneToken> countToken = new AtomicReference<>();
            AtomicBoolean rerouteCountFrame = new AtomicBoolean(true);

            return messages.handle((message, sink) -> {

                if (rerouteCountFrame.get() && AbstractDoneToken.hasCount(message)) {

                    if (countToken.get() == null) {
                        countToken.set((AbstractDoneToken) message);
                        return;
                    }

                    if (countToken.get() != null) {

                        rerouteCountFrame.set(false);
                        sink.next(countToken.get());
                        sink.complete();
                        return;
                    }
                }

                sink.next(message);
            });
        });
    }

    /**
     * Augment query for generated keys retrieval. Prior to augmenting, use {@link #shouldExpectGeneratedKeys(String[])} to check whether augmentation should apply.
     *
     * @param sql              the query to to augment.
     * @param generatedColumns column name for the generated keys. Can be {@code null}.
     * @return the potentially augmented query.
     * @see #shouldExpectGeneratedKeys(String[])
     */
    static String augmentQuery(String sql, @Nullable String[] generatedColumns) {

        if (shouldExpectGeneratedKeys(generatedColumns)) {
            return sql + " " + getGeneratedKeysClause(generatedColumns);
        }

        return sql;
    }

    /**
     * Returns {@literal true} whether {@code generatedColumns} indicate the caller to expect generated keys.
     *
     * @param generatedColumns column name for the generated keys. Can be {@code null}.
     * @return {@literal true} whether {@code generatedColumns} indicates that keys should be generated.
     */
    static boolean shouldExpectGeneratedKeys(@Nullable String[] generatedColumns) {
        return generatedColumns != null;
    }

    /**
     * Return the key generation clause. Allows column name customization by passing a single column name. Multiple column names are not supported.
     *
     * @param columns column names. Defaults to {@link #DEFAULT_GENERATED_KEY_NAME} if no column name specified.
     * @return the generated keys clause.
     * @throws IllegalArgumentException      if {@code columns} is {@code null}.
     * @throws UnsupportedOperationException if {@code columns} contains more than one column.
     */
    static String getGeneratedKeysClause(String... columns) {

        Assert.requireNonNull(columns, "Columns must not be null");

        if (columns.length == 0) {
            return GENERATED_KEYS_QUERY + DEFAULT_GENERATED_KEY_NAME;
        }

        if (columns.length == 1) {

            Assert.requireNonNull(columns[0], "Column must not be null");
            return GENERATED_KEYS_QUERY + columns[0];
        }

        throw new UnsupportedOperationException("SQL Server does not support multiple generated keys");
    }
}
