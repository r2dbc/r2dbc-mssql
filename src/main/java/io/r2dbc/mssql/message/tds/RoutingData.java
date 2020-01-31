package io.r2dbc.mssql.message.tds;

/**
 * Represents an alternative route.
 */
public final class RoutingData {

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

    RoutingData(String serverName, int port) {
        this.serverName = serverName;
        this.port = port;
    }
}
