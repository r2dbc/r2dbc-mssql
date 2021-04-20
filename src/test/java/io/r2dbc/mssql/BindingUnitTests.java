/*
 * Copyright 2018-2021 the original author or authors.
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
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.codec.Encoded;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.message.type.TdsDataType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Binding}.
 *
 * @author Mark Paluch
 */
class BindingUnitTests {

    @Test
    void shouldReportFormalParameters() {

        Binding binding = new Binding();
        binding.add("foo", RpcDirection.IN, Encoded.of(TdsDataType.INT8, Unpooled.EMPTY_BUFFER));

        assertThat(binding.getFormalParameters()).isEqualTo("@foo bigint");
    }

    @Test
    void shouldReportMultipleFormalParameters() {

        Binding binding = new Binding();
        binding.add("foo", RpcDirection.IN, Encoded.of(TdsDataType.INT8, Unpooled.EMPTY_BUFFER));
        binding.add("bar", RpcDirection.IN, Encoded.of(TdsDataType.MONEY8, Unpooled.EMPTY_BUFFER));

        assertThat(binding.getFormalParameters()).isEqualTo("@foo bigint,@bar money");
    }

    @Test
    void shouldReportCorrectIntermediateFormalParameters() {

        Binding binding = new Binding();

        binding.add("foo", RpcDirection.IN, Encoded.of(TdsDataType.INT8, Unpooled.EMPTY_BUFFER));
        assertThat(binding.getFormalParameters()).isEqualTo("@foo bigint");

        binding.add("bar", RpcDirection.IN, Encoded.of(TdsDataType.MONEY8, Unpooled.EMPTY_BUFFER));
        assertThat(binding.getFormalParameters()).isEqualTo("@foo bigint,@bar money");
    }

    @Test
    void shouldBindParameters() {

        Binding binding = new Binding();

        binding.add("foo", RpcDirection.IN, Encoded.of(TdsDataType.INT8, Unpooled.EMPTY_BUFFER));

        assertThat(binding.getParameters()).isNotEmpty();
        assertThat(binding.isEmpty()).isFalse();
        assertThat(binding.size()).isEqualTo(1);
    }

    @Test
    void shouldClearBindings() {

        Binding binding = new Binding();

        ByteBuf buffer = Unpooled.buffer();
        binding.add("foo", RpcDirection.IN, Encoded.of(TdsDataType.INT8, buffer));
        binding.clear();

        assertThat(binding.isEmpty()).isTrue();
        assertThat(buffer.refCnt()).isZero();
    }
}
