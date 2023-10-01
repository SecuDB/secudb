package com.secudb.api.exceptions;

public class ConflictException extends Exception {
    public ConflictException() {
        super("Conflict");
    }

    public ConflictException(String message) {
        super(message);
    }
}
