package com.tollmesh.store;

public enum ErrorCode {
    OK(0),
    INTERNAL(1),
    UNAVAILABLE(2),
    INVALID_ARGUMENT(3),
    DEADLINE_EXCEEDED(4),
    NOT_FOUND(5);

    private final int code;

    ErrorCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static ErrorCode fromCode(int code) {
        for (ErrorCode ec : values()) {
            if (ec.code == code) {
                return ec;
            }
        }
        return INTERNAL;
    }

    public static ErrorCode fromCode(String code) {
        try {
            return fromCode(Integer.parseInt(code));
        } catch (NumberFormatException e) {
            return INTERNAL;
        }
    }
}
