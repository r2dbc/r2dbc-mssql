package io.r2dbc.mssql.client;

import javax.annotation.Nullable;

/**
 * Represents the outcome of a login exchange.
 *
 * @author Lars Haatveit
 */
public class LoginExchangeResult {

    public enum Outcome {
        /**
         * The connection is ready.
         */
        CONNECTED,

        /**
         * The client was routed to a different server.
         */
        ROUTED
    }

    private final Outcome outcome;

    @Nullable
    private final String alternateServerName;

    private final int alternateServerPort;

    private LoginExchangeResult(Outcome outcome, @Nullable String alternateServerName, int alternateServerPort) {
        this.outcome = outcome;
        this.alternateServerName = alternateServerName;
        this.alternateServerPort = alternateServerPort;
    }

    /**
     * Get the outcome of the login exchange.
     *
     * @return the outcome
     */
    public Outcome getOutcome() {
        return outcome;
    }

    public static LoginExchangeResult connected() {
        return new LoginExchangeResult(Outcome.CONNECTED, null, 0);
    }

    public static LoginExchangeResult routed(String alternateServerName, int alternateServerPort) {
        return new LoginExchangeResult(Outcome.ROUTED, alternateServerName, alternateServerPort);
    }

    /**
     * Get the alternate server port.
     *
     * @return the port
     */
    public int getAlternateServerPort() {
        return alternateServerPort;
    }

    /**
     * Get the alternate server name.
     *
     * @return the server name
     */
    @Nullable
    public String getAlternateServerName() {
        return alternateServerName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return (obj instanceof LoginExchangeResult) && ((LoginExchangeResult) obj).getOutcome() == this.outcome;
    }

    @Override
    public int hashCode() {
        return this.outcome.hashCode();
    }
}
