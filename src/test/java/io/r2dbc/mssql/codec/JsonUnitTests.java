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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link Json}.
 */
class JsonUnitTests {

    private static final String JSON = "{\"a\":1}";

    @Test
    void shouldCreateFromString() {

        Json json = Json.of(JSON);

        assertThat(json.asString()).isEqualTo(JSON);
        assertThat(json.asArray()).isEqualTo(JSON.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldCreateFromByteArrayAndCopyIt() {

        byte[] bytes = JSON.getBytes(StandardCharsets.UTF_8);
        Json json = Json.of(bytes);

        bytes[0] = 'X';

        assertThat(json.asString()).isEqualTo(JSON);
    }

    @Test
    void shouldReturnCopyOfByteArray() {

        Json json = Json.of(JSON);

        json.asArray()[0] = 'X';

        assertThat(json.asString()).isEqualTo(JSON);
    }

    @Test
    void shouldCreateFromByteBufferWithoutChangingIt() {

        ByteBuffer buffer = ByteBuffer.wrap(JSON.getBytes(StandardCharsets.UTF_8));
        Json json = Json.of(buffer);

        assertThat(json.asString()).isEqualTo(JSON);
        assertThat(buffer.remaining()).isEqualTo(JSON.length());
    }

    @Test
    void shouldCreateFromByteBufAndReleaseIt() {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();
        buffer.writeBytes(JSON.getBytes(StandardCharsets.UTF_8));

        Json json = Json.of(buffer);

        assertThat(json.asString()).isEqualTo(JSON);
        assertThat(buffer.refCnt()).isEqualTo(0);
    }

    @Test
    void shouldCreateFromInputStreamAndCloseIt() {

        CloseTrackingInputStream stream = new CloseTrackingInputStream(JSON.getBytes(StandardCharsets.UTF_8));

        Json json = Json.of(stream);

        assertThat(json.asString()).isEqualTo(JSON);
        assertThat(stream.closed).isTrue();
    }

    @Test
    void shouldMapInputStream() {

        String value = Json.of(JSON).mapInputStream(in -> new Scanner(in, "UTF-8").useDelimiter("\\A").next());

        assertThat(value).isEqualTo(JSON);
    }

    @Test
    void shouldMapBuffer() {

        String value = Json.of(JSON).mapBuffer(buffer -> StandardCharsets.UTF_8.decode(buffer).toString());

        assertThat(value).isEqualTo(JSON);
    }

    @Test
    void shouldMapByteBuf() {

        String value = Json.of(JSON).mapByteBuf(buffer -> buffer.toString(StandardCharsets.UTF_8));

        assertThat(value).isEqualTo(JSON);
    }

    @Test
    void shouldBeConsumableMultipleTimes() {

        Json json = Json.of(JSON);

        String fromStream = json.mapInputStream(in -> new Scanner(in, "UTF-8").useDelimiter("\\A").next());

        assertThat(json.asString()).isEqualTo(JSON);
        assertThat(json.asString()).isEqualTo(JSON);
        assertThat(fromStream).isEqualTo(JSON);
    }

    @Test
    void shouldHandleNonAsciiCharacters() {

        Json json = Json.of("{\"name\":\"Zoë\"}");

        assertThat(json.asString()).isEqualTo("{\"name\":\"Zoë\"}");
    }

    @Test
    void shouldImplementEqualsAndHashCode() {

        assertThat(Json.of(JSON)).isEqualTo(Json.of(JSON));
        assertThat(Json.of(JSON).hashCode()).isEqualTo(Json.of(JSON).hashCode());
        assertThat(Json.of(JSON)).isNotEqualTo(Json.of("{\"b\":2}"));
    }

    @Test
    void shouldIncludeValueInToString() {

        assertThat(Json.of(JSON).toString()).contains(JSON);
    }

    @Test
    void shouldRejectNull() {

        assertThatThrownBy(() -> Json.of((String) null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Json.of((byte[]) null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Json.of((ByteBuffer) null)).isInstanceOf(IllegalArgumentException.class);
    }

    static class CloseTrackingInputStream extends ByteArrayInputStream {

        boolean closed;

        CloseTrackingInputStream(byte[] buf) {
            super(buf);
        }

        @Override
        public void close() {
            this.closed = true;
        }

    }

}