package com.secudb.storage.filesystem;

import com.secudb.storage.api.Storage;
import com.secudb.storage.api.StorageConnector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileSystemStorageConnector implements StorageConnector {
    @Override
    public String getName() {
        return "File System (Java NIO)";
    }

    @Override
    public Storage connect(String name, String connectionString) throws IOException {
        if (!supports(connectionString)) {
            throw new IllegalArgumentException();
        }

        String path = connectionString;
        if (connectionString.startsWith("file://")) {
            path = connectionString.substring(7);
        }

        Path rootPath = Paths.get(path);
        if (!Files.exists(rootPath)) {
            Files.createDirectories(rootPath);
        } else {
            if (!Files.isDirectory(rootPath)) {
                throw new IOException("Not a directory: " + rootPath);
            }
        }

        return new FileSystemStorage(name, rootPath);
    }

    @Override
    public boolean supports(String connectionString) {
        return connectionString.startsWith("file://") || connectionString.startsWith("/") || (connectionString.charAt(1) == ':' && connectionString.charAt(2) == '\\');
    }
}
