package com.secudb.storage.memory;

import com.secudb.commons.binary.Binary;
import com.secudb.commons.io.Bytes;
import com.secudb.storage.api.AbstractSyncStorage;
import com.secudb.storage.api.StorageReadOptions;
import com.secudb.storage.api.StorageWriteOptions;
import com.secudb.storage.api.exceptions.AlreadyExistsInStorageException;
import com.secudb.storage.api.exceptions.NotFoundInStorageException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStorage extends AbstractSyncStorage {
    private final String name;
    protected ConcurrentHashMap<String, byte[]> dataMap = new ConcurrentHashMap<>();

    public MemoryStorage(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean exists(String path) throws IOException {
        return dataMap.containsKey(path);
    }

    @Override
    public long length(String path) throws IOException {
        return dataMap.get(path).length;
    }

    @Override
    public void write(String path, Binary data, StorageWriteOptions options) throws AlreadyExistsInStorageException, IOException {
        byte[] inMemoryData = data.getBytesReadOnly();

        if (!options.isAllowOverwrite()) {
            byte[] current = dataMap.putIfAbsent(path, inMemoryData);
            if (current != null) {
                throw new AlreadyExistsInStorageException();
            }
        }
        else {
            dataMap.put(path, inMemoryData);
        }
    }

    @Override
    public byte[] readFully(String path, StorageReadOptions options) throws NotFoundInStorageException, IOException {
        byte[] data = dataMap.get(path);
        if (data == null) {
            throw new NotFoundInStorageException();
        }
        return Bytes.clone(data);
    }

    @Override
    public InputStream readStream(String path, StorageReadOptions options) throws NotFoundInStorageException, IOException {
        byte[] data = dataMap.get(path);
        if (data == null) {
            throw new NotFoundInStorageException();
        }

        InputStream inputStream = new ByteArrayInputStream(data);
        return options.applyOffsetAndLength(inputStream);
    }

    @Override
    public void delete(String path) throws NotFoundInStorageException, IOException {
        byte[] data = dataMap.remove(path);
        if (data == null) {
            throw new NotFoundInStorageException();
        }
    }

    @Override
    public void close() throws Exception {
    }
}
