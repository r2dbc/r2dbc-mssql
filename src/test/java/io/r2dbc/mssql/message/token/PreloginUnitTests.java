/*
 * Copyright 2018 the original author or authors.
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
package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.ContextualTdsFragment;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Prelogin}.
 * 
 * @author Mark Paluch
 */
final class PreloginUnitTests {

	@Test
	void shouldUsePreloginHeader() {

		Prelogin prelogin = Prelogin.builder().build();

		Mono.from(prelogin.encode(TestByteBufAllocator.TEST))
			.cast(ContextualTdsFragment.class)
			.as(StepVerifier::create)
			.consumeNextWith(actual -> {
				assertThat(actual.getHeaderOptions().getType()).isEqualTo(Type.PRE_LOGIN);
			}).verifyComplete();
	}
	
	@Test
	void shouldEncodePrelogin() {

		Prelogin prelogin = Prelogin.builder().build();
		ByteBuf buffer = Unpooled.buffer(32);

		prelogin.encode(buffer);

		String result = ByteBufUtil.prettyHexDump(buffer);

		// Version header at offset 10, length 6
		assertThat(result).containsSequence("00 00 10 00 06");
	}

	@Test
	void shouldEncodePreloginVersion() {

		Prelogin.Version token = new Prelogin.Version(0, 0);

		assertThat(token.getTokenHeaderLength()).isEqualTo(5);
		assertThat(token.getDataLength()).isEqualTo(6);

		ByteBuf buffer = Unpooled.buffer(11);

		token.encodeToken(buffer, 0);
		token.encodeStream(buffer);

		// Token header sequence
		assertThat(buffer.array()).startsWith(0, 0, 0, 0, 6);

		// Token data sequence
		assertThat(buffer.array()).endsWith(0, 0, 0, 0, 0, 0);
	}

	@Test
	void shouldEncodeEncryption() {

		Prelogin.Encryption token = new Prelogin.Encryption(Prelogin.Encryption.ENCRYPT_NOT_SUP);

		assertThat(token.getTokenHeaderLength()).isEqualTo(5);
		assertThat(token.getDataLength()).isEqualTo(1);

		ByteBuf buffer = Unpooled.buffer(6);

		token.encodeToken(buffer, 0);
		token.encodeStream(buffer);

		// Token header sequence
		assertThat(buffer.array()).startsWith(1, 0, 0, 0, 1);

		// Token data sequence
		assertThat(buffer.array()).endsWith(Prelogin.Encryption.ENCRYPT_NOT_SUP);
	}

	@Test
	void shouldDecodePreloginResponse() {

		// Server version 14.0.3038.14
		// Encryption disabled
		// Instance validation 0x00
		String response = "04 01 00 20 00 00 01 00 00 00 10 00 06 01 00 16 00 01 02 00 17 00 01 ff 0e 00 0b de 00 00 02 00"
				.replaceAll(" ", "");

		ByteBuf byteBuf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(response));
		Header header = Header.decode(byteBuf);
		Prelogin prelogin = Prelogin.decode(byteBuf);

		assertThat(prelogin.getTokens()).hasSize(4);
		assertThat(prelogin.getToken(Prelogin.Version.class)).isNotEmpty().hasValueSatisfying(actual -> {

			assertThat(actual.getVersion()).isEqualTo(14);
			assertThat(actual.getSubbuild()).isEqualTo((short) 3038);
		});

		assertThat(prelogin.getToken(Prelogin.Encryption.class)).isNotEmpty().hasValueSatisfying(actual -> {

			assertThat(actual.getEncryption()).isEqualTo(Prelogin.Encryption.ENCRYPT_NOT_SUP);
		});

		assertThat(prelogin.getToken(Prelogin.Terminator.class)).hasValue(Prelogin.Terminator.INSTANCE);
	}
}
