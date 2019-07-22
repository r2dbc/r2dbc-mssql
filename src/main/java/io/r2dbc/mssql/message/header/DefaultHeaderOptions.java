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

package io.r2dbc.mssql.message.header;

/**
 * Default implementation of {@link HeaderOptions}.
 *
 * @author Mark Paluch
 */
class DefaultHeaderOptions implements HeaderOptions {

    private static HeaderOptions[][] HEADER_CACHE = new HeaderOptions[Type.values().length][(-Byte.MIN_VALUE) + Byte.MAX_VALUE];

    static {
        for (Type value : Type.values()) {
            for (byte b = Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
                int index = b - Byte.MIN_VALUE;
                HEADER_CACHE[value.ordinal()][index] = new DefaultHeaderOptions(value, Status.fromBitmask(b));
            }
        }
    }

    static HeaderOptions get(Type value, Status status) {
        int index = status.getValue() - Byte.MIN_VALUE;
        return HEADER_CACHE[value.ordinal()][index];
    }

    private final Type type;

    private final Status status;

    DefaultHeaderOptions(Type type, Status status) {
        this.type = type;
        this.status = status;
    }

    @Override
    public Type getType() {
        return this.type;
    }

    @Override
    public Status getStatus() {
        return this.status;
    }

    @Override
    public boolean is(Status.StatusBit bit) {
        return this.status.is(bit);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [type=").append(this.type);
        sb.append(", status=").append(this.status);
        sb.append(']');
        return sb.toString();
    }
}
