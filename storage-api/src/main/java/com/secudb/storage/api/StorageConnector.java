package com.secudb.storage.api;

import java.io.IOException;

public interface StorageConnector {
    String getName();

    Storage connect(String name, String connectionString) throws IOException;

    boolean supports(String connectionString);
}
