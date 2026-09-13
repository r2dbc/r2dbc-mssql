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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SqlServerBrowserRequestEncoder}.
 *
 * @author Daniel Pachali
 */
final class SqlServerBrowserRequestEncoderUnitTests {

    @Test
    void shouldEncodeNamedInstanceRequest() {

        ByteBuf buffer = Unpooled.buffer();

        SqlServerBrowserRequestEncoder.encode(
            buffer, "SQLEXPRESS", StandardCharsets.US_ASCII);

        assertThat(buffer.writerIndex()).isEqualTo(12);

        assertThat(ByteBufUtil.hexDump(buffer))
            .isEqualToIgnoringCase("0453514c4558505245535300");
    }

    @Test
    void shouldEncodeInstanceNameContainingAllowedCharacters() {

        ByteBuf buffer = Unpooled.buffer();

        SqlServerBrowserRequestEncoder.encode(
            buffer, "SQL_2026$", StandardCharsets.US_ASCII);

        assertThat((int) buffer.readUnsignedByte()).isEqualTo(0x04);

        assertThat(buffer.readCharSequence(
            buffer.readableBytes() - 1, StandardCharsets.US_ASCII))
            .hasToString("SQL_2026$");

        assertThat(buffer.readUnsignedByte()).isZero();
    }

    @Test
    void shouldEncodeMaximumLengthInstanceName() {

        ByteBuf buffer = Unpooled.buffer();

        String instanceName = repeat('A', 32);

        SqlServerBrowserRequestEncoder.encode(
            buffer, instanceName, StandardCharsets.US_ASCII);

        assertThat(buffer.writerIndex()).isEqualTo(34);
        assertThat((int) buffer.getUnsignedByte(0)).isEqualTo(0x04);
        assertThat(buffer.getUnsignedByte(33)).isZero();
    }

    @Test
    void shouldRejectInstanceNameExceedingMaximumLength() {

        ByteBuf buffer = Unpooled.buffer();

        String instanceName = repeat('A', 33);

        assertThatThrownBy(() ->
            SqlServerBrowserRequestEncoder.encode(
                buffer, instanceName, StandardCharsets.US_ASCII))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must not exceed 32 bytes");
    }

    @Test
    void shouldRejectEmptyInstanceName() {

        ByteBuf buffer = Unpooled.buffer();

        assertThatThrownBy(() ->
            SqlServerBrowserRequestEncoder.encode(
                buffer, "", StandardCharsets.US_ASCII))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must not be empty");
    }

    @Test
    void shouldRejectInstanceNameContainingNullCharacter() {

        ByteBuf buffer = Unpooled.buffer();

        assertThatThrownBy(() ->
            SqlServerBrowserRequestEncoder.encode(
                buffer, "SQL\0EXPRESS", StandardCharsets.US_ASCII))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must not contain a null character");
    }

    @Test
    void shouldRejectInstanceNameNotRepresentableByCharset() {

        ByteBuf buffer = Unpooled.buffer();

        assertThatThrownBy(() ->
            SqlServerBrowserRequestEncoder.encode(
                buffer, "MÜNCHEN", StandardCharsets.US_ASCII))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot be encoded using charset");
    }

    @Test
    void shouldRejectNullArguments() {

        ByteBuf buffer = Unpooled.buffer();

        assertThatThrownBy(() ->
            SqlServerBrowserRequestEncoder.encode(
                null, "SQLEXPRESS", StandardCharsets.US_ASCII))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("buffer must not be null");

        assertThatThrownBy(() ->
            SqlServerBrowserRequestEncoder.encode(
                buffer, null, StandardCharsets.US_ASCII))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("instanceName must not be null");

        assertThatThrownBy(() ->
            SqlServerBrowserRequestEncoder.encode(
                buffer, "SQLEXPRESS", null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("charset must not be null");
    }

    private static String repeat(char character, int count) {

        char[] characters = new char[count];

        Arrays.fill(characters, character);

        return new String(characters);
    }

}