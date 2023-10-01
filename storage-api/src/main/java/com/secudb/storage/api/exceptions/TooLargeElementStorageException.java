package com.secudb.storage.api.exceptions;

import java.io.IOException;

public class TooLargeElementStorageException extends IOException {

    public TooLargeElementStorageException() {
        this("Element is too large");
    }

    public TooLargeElementStorageException(String message) {
        super(message);
    }

    public TooLargeElementStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public TooLargeElementStorageException(Throwable cause) {
        super(cause);
    }
}
