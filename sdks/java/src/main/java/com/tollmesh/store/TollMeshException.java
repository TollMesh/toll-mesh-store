package com.tollmesh.store;

public class TollMeshException extends Exception {
    private final ErrorCode errorCode;
    private final int statusCode;

    public TollMeshException(String message) {
        super(message);
        this.errorCode = ErrorCode.INTERNAL;
        this.statusCode = 0;
    }

    public TollMeshException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = ErrorCode.INTERNAL;
        this.statusCode = 0;
    }

    public TollMeshException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.statusCode = errorCode.getCode();
    }

    public TollMeshException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.statusCode = errorCode.getCode();
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
