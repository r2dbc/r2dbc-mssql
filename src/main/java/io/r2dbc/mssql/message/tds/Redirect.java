package io.r2dbc.mssql.message.tds;

import io.netty.buffer.ByteBuf;

/**
 * Represents a client redirection to a different server.
 */
public final class Redirect {

    private final String serverName;

    private final int port;

    /**
     * Get the alternate server name.
     *
     * @return the server name
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * Get the alternate port.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    Redirect(String serverName, int port) {
        this.serverName = serverName;
        this.port = port;
    }

    /**
     * Decode a {@link Redirect} from {@link ByteBuf}.
     *
     * @param buffer the data buffer
     * @return the decoded {@link Redirect}
     */
    public static Redirect decode(ByteBuf buffer) {

        final int PROTOCOL_TCP_IP = 0;

        int routingDataValueLength = buffer.readUnsignedShortLE();

        if (routingDataValueLength <= 5) {
            throw new IllegalArgumentException("Decoding error, buffer is too short.");
        }

        int protocol = buffer.readUnsignedByte();

        if (protocol != PROTOCOL_TCP_IP) {
            throw new IllegalArgumentException("Unknown route protocol.");
        }

        // The ProtocolProperty field represents the remote port when the protocol is TCP/IP.
        // https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/2b3eb7e5-d43d-4d1b-bf4d-76b9e3afc791

        int port = buffer.readUnsignedShortLE();
        String serverName = Decode.unicodeUString(buffer);

        return new Redirect(serverName, port);
    }
}
