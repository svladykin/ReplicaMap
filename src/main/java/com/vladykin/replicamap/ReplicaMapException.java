package com.vladykin.replicamap;

/**
 * Runtime exception that is typically thrown in case of failures.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class ReplicaMapException extends RuntimeException {
    static final long serialVersionUID = 1L;

    public ReplicaMapException(String message) {
        super(message);
    }

    public ReplicaMapException(Throwable e) {
        super(e);
    }

    public ReplicaMapException(String message, Throwable cause) {
        super(message, cause);
    }
}
