package com.secudb.storage.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

public class StorageConnectors implements Iterable<StorageConnector> {
    private Iterable<StorageConnector> connectors;

    public StorageConnectors() {
        this(ServiceLoader.load(StorageConnector.class));
    }

    public StorageConnectors(Iterable<StorageConnector> connectors) {
        this.connectors = connectors;
    }

    @Override
    public Iterator<StorageConnector> iterator() {
        return connectors.iterator();
    }

    public long count() {
        long count = 0;
        Iterator<StorageConnector> iterator = this.iterator();
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        return count;
    }

    public Optional<StorageConnector> find(String connectionString) {
        for (StorageConnector connector : this) {
            if (connector.supports(connectionString)) {
                return Optional.of(connector);
            }
        }
        return Optional.empty();
    }

    public Optional<Storage> connect(String name, String connectionString) throws IOException {
        for (StorageConnector connector : this) {
            if (connector.supports(connectionString)) {
                return Optional.of(
                    connector.connect(name, connectionString)
                );
            }
        }
        return Optional.empty();
    }
}
