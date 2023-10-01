package com.secudb.storage.memory;

import com.secudb.storage.api.Storage;
import com.secudb.storage.api.StorageConnector;

import java.util.concurrent.ConcurrentHashMap;

public class MemoryStorageConnector implements StorageConnector {
    private ConcurrentHashMap<String, MemoryStorage> storages = new ConcurrentHashMap<>();

    @Override
    public String getName() {
        return "Memory (Java Heap)";
    }

    @Override
    public Storage connect(String name, String connectionString) {
        if (!supports(connectionString)) {
            throw new IllegalArgumentException();
        }
        return storages.computeIfAbsent(connectionString.substring(3), x -> new MemoryStorage(name));
    }

    @Override
    public boolean supports(String connectionString) {
        return connectionString.equals("mem") || connectionString.startsWith("mem:");
    }

    public void clearAll() {
        storages.clear();
    }
}
