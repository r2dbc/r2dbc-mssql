/*
 * Copyright 2019-2022 the original author or authors.
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MssqlConnectionMetadata}
 *
 * @author Mark Paluch
 */
class MssqlConnectionMetadataUnitTests {

    @Test
    void shouldConstructMetadata() {

        String edition = "Developer Edition (64-bit)";
        String version = "14.0.3162.1";
        String versionString =
            "Microsoft SQL Server 2017 (RTM-CU15) (KB4498951) - 14.0.3162.1 (X64) " +
                "May 15 2019 19:14:30" +
                "Copyright (C) 2017 Microsoft Corporation" +
                "Developer Edition (64-bit) on Linux (Ubuntu 16.04.6 LTS)";

        MssqlConnectionMetadata metadata = MssqlConnectionMetadata.from(edition, version, versionString);

        assertThat(metadata.getDatabaseProductName()).isEqualTo("Microsoft SQL Server 2017 (RTM-CU15) (KB4498951) - Developer Edition (64-bit)");
        assertThat(metadata.getDatabaseVersion()).isEqualTo(version);
    }
}
