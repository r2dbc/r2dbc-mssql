/*
 * Copyright 2018-2020 the original author or authors.
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

import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.Redirect;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.Prelogin;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.token.RowTokenFactory;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link MssqlConnectionFactory}.
 *
 * @author Mark Paluch
 */
final class MssqlConnectionFactoryUnitTests {

    MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder().host("initial").username("user").password("password").build();

    static final Column[] COLUMNS = Arrays.asList(
        createColumn(0, "Edition", SqlServerType.NVARCHAR, 100, LengthStrategy.USHORTLENTYPE, ServerCharset.UNICODE.charset()),
        createColumn(1, "VersionString", SqlServerType.VARCHAR, 100, LengthStrategy.USHORTLENTYPE, ServerCharset.CP1252.charset())).toArray(new Column[0]);

    @Test
    void constructorNoClientFactory() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MssqlConnectionFactory(null, MssqlConnectionConfiguration.builder()
            .host("test-host")
            .password("test-password")
            .username("test-username")
            .build()))
            .withMessage("clientFactory must not be null");
    }

    @Test
    void constructorNoConfiguration() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MssqlConnectionFactory(null))
            .withMessage("configuration must not be null");
    }

    @Test
    void shouldFollowRedirect() {

        ColumnMetadataToken columns = ColumnMetadataToken.create(COLUMNS);
        RowToken rowToken = RowTokenFactory.create(columns, buffer -> {
            Encode.uString(buffer, "Edition", ServerCharset.UNICODE.charset());
            Encode.uString(buffer, "1.2.3", ServerCharset.CP1252.charset());
        });

        TestClient initial =
            TestClient.builder().expectClose().withRedirect(Redirect.create("redirect", 1234)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(DoneToken.create(0)).build();


        TestClient redirect =
            TestClient.builder().assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(DoneToken.create(0)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(SqlBatch.class);
            }).thenRespond(columns, rowToken, DoneToken.create(1)).build();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> {

            if (config.getHost().equals("initial")) {
                return Mono.just(initial);
            }

            return Mono.just(redirect);
        }, configuration);


        connectionFactory.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();

        assertThat(initial.isClosed()).isTrue();
        assertThat(redirect.isClosed()).isFalse();
    }

    @Test
    void properlyPropagatesFailures() {

        ErrorToken error = new ErrorToken(0, 0, (byte) 0, (byte) 0, "failure", "", "", 0);

        TestClient initial =
            TestClient.builder().expectClose().withRedirect(Redirect.create("redirect", 1234)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(error).build();


        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> {
            return Mono.just(initial);
        }, configuration);


        connectionFactory.create().as(StepVerifier::create).verifyError(R2dbcNonTransientResourceException.class);

        assertThat(initial.isClosed()).isTrue();
    }

    @Test
    void shouldFailOnMultipleRedirects() {

        TestClient initial =
            TestClient.builder().expectClose().withRedirect(Redirect.create("redirect", 1234)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(DoneToken.create(0)).build();


        TestClient redirect =
            TestClient.builder().expectClose().withRedirect(Redirect.create("redirect", 1234)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(DoneToken.create(0)).build();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> {

            if (config.getHost().equals("initial")) {
                return Mono.just(initial);
            }

            return Mono.just(redirect);
        }, configuration);


        connectionFactory.create().as(StepVerifier::create).verifyError(MssqlConnectionFactory.MssqlRoutingException.class);

        assertThat(initial.isClosed()).isTrue();
        assertThat(redirect.isClosed()).isTrue();
    }

    private static Column createColumn(int index, String name, SqlServerType serverType, int length, LengthStrategy lengthStrategy, @Nullable Charset charset) {

        TypeInformation.Builder builder = TypeInformation.builder().withServerType(serverType).withMaxLength(length).withLengthStrategy(lengthStrategy);
        if (charset != null) {
            builder.withCharset(charset);
        }
        TypeInformation type = builder.build();

        return new Column(index, name, type, null);
    }
}
