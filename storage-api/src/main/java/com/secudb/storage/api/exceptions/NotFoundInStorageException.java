package com.secudb.storage.api.exceptions;

import java.io.IOException;

public class NotFoundInStorageException extends IOException {
    public NotFoundInStorageException() {
        this("Not found in storage");
    }

    public NotFoundInStorageException(String message) {
        super(message);
    }

    public NotFoundInStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotFoundInStorageException(Throwable cause) {
        super(cause);
    }
}
