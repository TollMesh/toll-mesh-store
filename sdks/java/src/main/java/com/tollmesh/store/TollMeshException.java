package com.tollmesh.store;

public class TollMeshException extends Exception {
    private final String errorCode;
    private final int statusCode;

    public TollMeshException(String message) {
        super(message);
        this.errorCode = null;
        this.statusCode = 0;
    }

    public TollMeshException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = null;
        this.statusCode = 0;
    }

    public TollMeshException(String message, String errorCode, int statusCode) {
        super(message);
        this.errorCode = errorCode;
        this.statusCode = statusCode;
    }

    public TollMeshException(String message, String errorCode, int statusCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.statusCode = statusCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
