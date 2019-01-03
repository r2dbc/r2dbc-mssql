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

package io.r2dbc.mssql.message.header;

import io.r2dbc.mssql.util.Assert;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Packet header status as defined in ch {@literal 2.2.3.1.2 Status} of the TDS v20180912 spec.
 * <p/>
 * Status is a bit field used to indicate the message state. Status is a 1-byte unsigned char. The following Status bit
 * flags are defined.
 *
 * @author Mark Paluch
 * @see StatusBit
 */
public class Status {

    private static final Status EMPTY = new Status(EnumSet.noneOf(StatusBit.class));

    private final Set<StatusBit> statusBits;

    private final byte value;

    private Status(Set<StatusBit> statusBits) {
        this.statusBits = statusBits;
        this.value = getStatusValue(statusBits);
    }

    /**
     * Return an empty {@link Status}.
     *
     * @return the empty {@link Status}.
     */
    public static Status empty() {
        return EMPTY;
    }

    /**
     * Create {@link StatusBit} {@link Set} from the given {@code bitmask}.
     *
     * @param bitmask
     * @return
     */
    public static Status fromBitmask(byte bitmask) {

        EnumSet<StatusBit> result = EnumSet.noneOf(StatusBit.class);

        for (StatusBit status : StatusBit.values()) {
            if ((bitmask & status.getBits()) != 0) {
                result.add(status);
            }
        }

        return new Status(result);
    }

    /**
     * Create a {@link Status} from the given {@link StatusBit}.
     *
     * @param bit the status bit.
     * @return the {@link Status} from the given {@link StatusBit}.
     */
    public static Status of(StatusBit bit) {

        Assert.requireNonNull(bit, "StatusBit must not be null");

        return new Status(EnumSet.of(bit));
    }

    /**
     * Create a {@link Status} from the given {@link StatusBit}s.
     *
     * @param bit   the status bit.
     * @param other the status bits.
     * @return the {@link Status} from the given {@link StatusBit}.
     */
    public static Status of(StatusBit bit, StatusBit... other) {

        Assert.requireNonNull(bit, "StatusBit must not be null");
        Assert.requireNonNull(other, "StatusBits must not be null");

        return new Status(EnumSet.of(bit, other));
    }

    /**
     * Create a {@link Status} from the current state and add the {@link StatusBit}.
     *
     * @param bit the status bit.
     * @return the {@link Status} from the given {@link StatusBit}.
     */
    public Status and(StatusBit bit) {

        Assert.requireNonNull(bit, "StatusBit must not be null");

        // If bit set, then we can optimize.
        if (this.statusBits.contains(bit)) {
            return this;
        }

        EnumSet<StatusBit> statusBits = EnumSet.copyOf(this.statusBits);
        statusBits.add(bit);

        return new Status(statusBits);
    }

    /**
     * Create a {@link Status} from the current state and remove the {@link StatusBit}.
     *
     * @param bit the status bit.
     * @return the {@link Status} from the given {@link StatusBit}.
     */
    public Status not(StatusBit bit) {

        Assert.requireNonNull(bit, "StatusBit must not be null");

        // If bit not set, then we can optimize.
        if (!this.statusBits.contains(bit)) {
            return this;
        }

        EnumSet<StatusBit> statusBits = EnumSet.copyOf(this.statusBits);
        statusBits.remove(bit);

        return new Status(statusBits);
    }

    /**
     * Check if the header status has set the {@link Status.StatusBit}.
     *
     * @param bit the status bit.
     * @return {@literal true} of the bit is set; {@literal false} otherwise.
     */
    public boolean is(Status.StatusBit bit) {

        Assert.requireNonNull(bit, "StatusBit must not be null");

        return this.statusBits.contains(bit);
    }

    /**
     * @return the status byte.
     */
    public byte getValue() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Status)) {
            return false;
        }
        Status status = (Status) o;
        return this.value == status.value &&
            Objects.equals(this.statusBits, status.statusBits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.statusBits, this.value);
    }

    private static byte getStatusValue(Collection<StatusBit> statusBits) {

        byte result = 0;

        for (Status.StatusBit s : statusBits) {
            result |= s.getBits();
        }

        return result;
    }

    @Override
    public String toString() {
        return Integer.toHexString(this.value);
    }

    /**
     * Packet header status bits as defined in ch {@literal 2.2.3.1.2 Status} of the TDS v20180912 spec.
     * <p/>
     * Status is a bit field used to indicate the message state. Status is a 1-byte unsigned char. The following Status
     * bit flags are defined.
     */
    public enum StatusBit {

        NORMAL(0x00), EOM(0x01), IGNORE(0x02),

        /**
         * RESETCONNECTION
         *
         * @since TDS 7.1
         */
        RESET_CONNECTION(0x08),

        /**
         * RESETCONNECTIONSKIPTRAN
         *
         * @since TDS 7.3
         */
        RESET_CONNECTION_SKIP_TRAN(0x10);

        StatusBit(int bits) {
            this.bits = Integer.valueOf(bits).byteValue();
        }

        private final byte bits;

        public int getBits() {
            return this.bits;
        }
    }
}
