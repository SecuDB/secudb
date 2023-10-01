package com.secudb.storage.api;

import com.secudb.commons.binary.Binary;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractAsyncStorage implements Storage {

    @Override
    public boolean isAsyncPreferred() {
        return true;
    }

    @Override
    public boolean exists(String path) throws IOException {
        return existsAsync(path).join();
    }

    @Override
    public long length(String path) throws IOException {
        return lengthAsync(path).join();
    }

    @Override
    public void testReadWrite() throws IOException {
        testReadWriteAsync().join();
    }

    @Override
    public CompletableFuture<Void> testReadWriteAsync() {
        throw new UnsupportedOperationException(); //TODO: implement similarly to AbstractSyncStorage
    }

    @Override
    public Binary readBinary(String path, StorageReadOptions options) throws IOException {
        return readBinaryAsync(path, options).join();
    }

    @Override
    public byte[] readFully(String path, StorageReadOptions options) throws IOException {
        return readFullyAsync(path, options).join();
    }

    @Override
    public InputStream readStream(String path, StorageReadOptions options) throws IOException {
        return readStreamAsync(path, options).join();
    }

    @Override
    public void write(String path, Binary data, StorageWriteOptions options) throws IOException {
        writeAsync(path, data, options).join();
    }

    @Override
    public void delete(String path) throws IOException {
        deleteAsync(path).join();
    }
}
