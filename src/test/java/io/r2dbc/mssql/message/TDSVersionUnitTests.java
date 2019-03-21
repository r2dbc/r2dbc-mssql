/*
 * Copyright 2018 the original author or authors.
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

package io.r2dbc.mssql.message;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TDSVersion}.
 *
 * @author Mark Paluch
 */
final class TDSVersionUnitTests {

    @Test
    void shouldCompareGreaterVersion() {

        assertThat(TDSVersion.VER_KATMAI.isGreateOrEqualsTo(TDSVersion.VER_YUKON)).isTrue();
        assertThat(TDSVersion.VER_KATMAI.isGreateOrEqualsTo(TDSVersion.VER_KATMAI)).isTrue();
        assertThat(TDSVersion.VER_KATMAI.isGreateOrEqualsTo(TDSVersion.VER_DENALI)).isFalse();
    }
}
