/*
 * Copyright 2018-2019 the original author or authors.
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

package io.r2dbc.mssql.message.type;

/**
 * SQL Server length strategies.
 */
public enum LengthStrategy {

    /**
     * Fixed-length type such as {@code NULL}, {@code INTn}, {@code MONEY}.
     */
    FIXEDLENTYPE,

    /**
     * Variable length type such as {@code NUMERICN} using a single {@code byte} as length
     * descriptor (0-255).
     */
    BYTELENTYPE,

    /**
     * Variable length type such as {@code VARCHAR}, {@code VARBINARY} (2 bytes) as length
     * descriptor (0-65534), {@code -1} represents {@code null}
     */
    USHORTLENTYPE,

    /**
     * Variable length type such as {@code TEXT} and  {@code IMAGE} using a {@code long} (4 bytes) as length
     * descriptor (0-2GB), {@code -1} represents {@code null}.
     */
    LONGLENTYPE,

    /**
     * Partially length type such as {@code BIGVARCHARTYPE}, {@code UDTTYYPE}, {@code NVARCHARTYPE} using a {@code short} as length
     * descriptor (0-8000).
     */
    PARTLENTYPE
}
