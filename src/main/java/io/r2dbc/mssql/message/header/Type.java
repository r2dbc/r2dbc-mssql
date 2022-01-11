/*
 * Copyright 2018-2022 the original author or authors.
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

package io.r2dbc.mssql.message.header;

/**
 * Packet header type as defined in ch {@literal 2.2.3.1.1 Type} of the TDS v20180912 spec.
 * <p>
 * Type defines the type of message. Type is a 1-byte unsigned char.
 */
public enum Type {

    SQL_BATCH(1), PRE_TDS7_LOGIN(2), RPC(3), TABULAR_RESULT(4), ATTENTION(6), BULK_LOAD_DATA(7), FED_AUTH_TOKEN(
        8), TX_MGR(14), TDS7_LOGIN(16), SSPI(17), PRE_LOGIN(18);

    Type(int value) {
        this.value = Integer.valueOf(value).byteValue();
    }

    private final byte value;

    /**
     * Resolve header {@code value} into {@link Type}.
     *
     * @param value packet type identifier.
     * @return the resolved {@link Type}.
     */
    public static Type valueOf(byte value) {

        switch (value) {
            case 1:
                return SQL_BATCH;
            case 2:
                return PRE_TDS7_LOGIN;
            case 3:
                return RPC;
            case 4:
                return TABULAR_RESULT;
            case 6:
                return ATTENTION;
            case 7:
                return BULK_LOAD_DATA;
            case 8:
                return FED_AUTH_TOKEN;
            case 14:
                return TX_MGR;
            case 16:
                return TDS7_LOGIN;
            case 17:
                return SSPI;
            case 18:
                return PRE_LOGIN;
        }

        throw new IllegalArgumentException(String.format("Invalid header type: 0x%01X", value));
    }

    public byte getValue() {
        return this.value;
    }
}
