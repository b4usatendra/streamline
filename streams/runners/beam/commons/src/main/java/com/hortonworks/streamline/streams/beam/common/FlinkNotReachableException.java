package com.hortonworks.streamline.streams.beam.common;

public class FlinkNotReachableException extends RuntimeException {

    public FlinkNotReachableException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlinkNotReachableException(String message) {
        super(message);
    }
}
