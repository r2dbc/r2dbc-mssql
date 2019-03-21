/*
 * Copyright 2018-2019 the original author or authors.
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

package io.r2dbc.mssql.util;

import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

/**
 * Type collection for testing.
 *
 * @author Mark Paluch
 */
public class Types {

    private static final TypeInformation integer =
        TypeInformation.builder().withScale(4).withMaxLength(4).withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.INTEGER).build();

    public static TypeInformation integer() {
        return integer;
    }

    public static TypeInformation varchar(int length) {
        return TypeInformation.builder().withServerType(SqlServerType.VARCHAR).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withMaxLength(length).build();
    }

    private Types() {

    }
}
