package com.secudb.storage.api.exceptions;

import java.io.IOException;

public class AlreadyExistsInStorageException extends IOException {
    public AlreadyExistsInStorageException() {
        this("Already exists in storage");
    }

    public AlreadyExistsInStorageException(String message) {
        super(message);
    }

    public AlreadyExistsInStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public AlreadyExistsInStorageException(Throwable cause) {
        super(cause);
    }
}
