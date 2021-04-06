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

package io.r2dbc.mssql.message;

import io.r2dbc.mssql.util.Assert;

/**
 * TDS protocol versions.
 *
 * @author Mark Paluch
 */
public enum TDSVersion {

    // TDS 7.4
    VER_DENALI(0x74000004),

    // TDS 7.3B(includes null bit compression)
    VER_KATMAI(0x730B0003),

    // TDS 7.2
    VER_YUKON(0x72090002),

    UNKNOWN(0x00000000);

    private final int version;

    TDSVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return this.version;
    }

    /**
     * Check is the reference {@link TDSVersion} is greater or equal to {@code this} version.
     *
     * @param reference the reference version.
     * @return {@code true} if the reference {@link TDSVersion} is greater or equal to {@code this} version.
     */
    public boolean isGreateOrEqualsTo(TDSVersion reference) {

        Assert.requireNonNull(reference, "Reference version must not be null");
        return getVersion() >= reference.getVersion();
    }
}
