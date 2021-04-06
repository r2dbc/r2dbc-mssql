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

package io.r2dbc.mssql.message.header;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.Assert;

import java.util.Objects;

/**
 * A header exchanged between client and server.
 */
public class Header implements HeaderOptions {

    /**
     * Number of bytes required to represent the header.
     */
    public static final int LENGTH = 8;

    /**
     * Type defines the type of message. 1-byte.
     */
    private final Type type;

    /**
     * Status is a bit field used to indicate the message state. 1-byte.
     */
    private final Status status;

    /**
     * Length is the size of the packet including the 8 bytes in the packet header. It is the number of bytes from the
     * start of this header to the start of the next packet header. Length is a 2-byte, unsigned short int and is
     * represented in network byte order (big-endian).
     * <p/>
     * Starting with TDS 7.3, the Length MUST be the negotiated packet size when sending a packet from client to server,
     * unless it is the last packet of a request (that is, the EOM bit in Status is ON), or the client has not logged in.
     */
    private final short length;

    /**
     * Spid is the process ID on the server, corresponding to the current connection.
     * <p/>
     * This information is sent by the server to the client and is useful for identifying which thread on the server sent
     * the TDS packet. It is provided for debugging purposes. The client MAY send the SPID value to the server. If the
     * client does not, then a value of {@code 0x0000} SHOULD be sent to the server. This is a 2-byte value and is
     * represented in network byte order (big-endian).
     */
    private final short spid;

    /**
     * PacketID is used for numbering message packets that contain data in addition to the packet header.
     * <p/>
     * PacketID is a 1-byte, unsigned char. Each time packet data is sent, the value of PacketID is incremented by 1,
     * modulo 256.<7> This allows the receiver to track the sequence of TDS packets for a given message. This value is
     * currently ignored.
     */
    private final byte packetId;

    /**
     * This 1 byte is currently not used. This byte SHOULD be set to 0x00 and SHOULD be ignored by the receiver.
     */
    private final byte window;

    public Header(Type type, Status status, int length, int spid) {
        this(type, status, (short) length, (short) spid, (byte) 0, (byte) 0);
    }

    public Header(Type type, Status status, int length, int spid, int packetId, int window) {
        this(type, status, (short) length, (short) spid, (byte) packetId, (byte) window);
    }

    public Header(Type type, Status status, short length, short spid, byte packetId, byte window) {

        Assert.requireNonNull(type, "Type must not be null");
        Assert.requireNonNull(status, "sStatus must not be null");
        Assert.isTrue(length >= 8, "Header length must be greater or equal to 8");

        this.type = type;
        this.status = status;
        this.length = length;
        this.spid = spid;
        this.packetId = packetId;
        this.window = window;
    }

    /**
     * Create a {@link Header} given {@link HeaderOptions}, packet {@code length}, and {@link PacketIdProvider}.
     *
     * @param options          the {@link HeaderOptions}.
     * @param length           packet length.
     * @param packetIdProvider the {@link PacketIdProvider}.
     * @return the {@link Header}.
     * @throws IllegalArgumentException when {@link HeaderOptions} or {@link PacketIdProvider} is {@code null}.
     */
    public static Header create(HeaderOptions options, int length, PacketIdProvider packetIdProvider) {

        Assert.requireNonNull(options, "HeaderOptions must not be null");
        Assert.requireNonNull(packetIdProvider, "PacketIdProvider must not be null");

        return new Header(options.getType(), options.getStatus(), length, 0, packetIdProvider.nextPacketId(), 0);
    }

    public Type getType() {
        return this.type;
    }

    public Status getStatus() {
        return this.status;
    }

    public boolean is(Status.StatusBit bit) {
        return this.status.is(bit);
    }

    public short getSpid() {
        return this.spid;
    }

    public byte getPacketId() {
        return this.packetId;
    }

    public byte getWindow() {
        return this.window;
    }

    public short getLength() {
        return this.length;
    }

    /**
     * Encode a header into a {@link ByteBuf}.
     *
     * @param buffer the target {@link ByteBuf}.
     */
    public void encode(ByteBuf buffer) {
        encode(buffer, this.type, this.status, this.length, this.spid, this.packetId, this.window);
    }

    /**
     * Encode a header into a {@link ByteBuf}.
     *
     * @param buffer           the target {@link ByteBuf}.
     * @param packetIdProvider must not be {@code null}.
     * @throws IllegalArgumentException when {@link HeaderOptions} or {@link PacketIdProvider} is {@code null}.
     */
    public void encode(ByteBuf buffer, PacketIdProvider packetIdProvider) {
        encode(buffer, this.type, this.status, this.length, this.spid, packetIdProvider.nextPacketId(), this.window);
    }

    /**
     * Encode a header into a {@link ByteBuf}.
     *
     * @param buffer           the target {@link ByteBuf}.
     * @param options          header options.
     * @param length           packet length.
     * @param packetIdProvider must not be {@code null}.
     * @throws IllegalArgumentException when {@link HeaderOptions} or {@link PacketIdProvider} is {@code null}.
     */
    public static void encode(ByteBuf buffer, HeaderOptions options, int length, PacketIdProvider packetIdProvider) {
        encode(buffer, options.getType(), options.getStatus(), length, (short) 0, packetIdProvider.nextPacketId(), (byte) 0);
    }

    /**
     * Encode the {@link Header}.
     *
     * @param buffer   the target {@link ByteBuf}.
     * @param type     packet type.
     * @param status   fragmentation/message status.
     * @param length   packet length.
     * @param spid     the spid (unused).
     * @param packetId the packet Id.
     * @param window   the window (unused).
     */
    public static void encode(ByteBuf buffer, Type type, Status status, int length, short spid, byte packetId, byte window) {

        buffer.ensureWritable(8);

        buffer.writeByte(type.getValue());
        buffer.writeByte(status.getValue());
        buffer.writeShort(length);
        buffer.writeShort(spid);
        buffer.writeByte(packetId);
        buffer.writeByte(window);
    }

    /**
     * @param buffer the data buffer to inspect.
     * @return {@code true} if the header can be decoded.
     */
    public static boolean canDecode(ByteBuf buffer) {
        return buffer.readableBytes() >= LENGTH;
    }

    /**
     * @param buffer the data buffer.
     * @return the decoded {@link Header}.
     */
    public static Header decode(ByteBuf buffer) {

        Type type = Type.valueOf(buffer.readByte());
        Status status = Status.fromBitmask(buffer.readByte());
        short length = buffer.readShort();
        short spid = buffer.readShort();
        byte packetId = buffer.readByte();
        byte window = buffer.readByte();

        return new Header(type, status, length, spid, packetId, window);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Header)) {
            return false;
        }
        Header header = (Header) o;
        return this.length == header.length &&
            this.spid == header.spid &&
            this.packetId == header.packetId &&
            this.window == header.window &&
            this.type == header.type &&
            Objects.equals(this.status, header.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.type, this.status, this.length, this.spid, this.packetId, this.window);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [type=").append(this.type);
        sb.append(", status=").append(this.status);
        sb.append(", length=").append(this.length);
        sb.append(", spid=").append(this.spid);
        sb.append(", packetId=").append(this.packetId);
        sb.append(", window=").append(this.window);
        sb.append(']');
        return sb.toString();
    }

}
