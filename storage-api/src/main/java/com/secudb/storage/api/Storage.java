package com.secudb.storage.api;

import com.secudb.commons.binary.Binary;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

public interface Storage extends AutoCloseable {
    String getName();

    boolean isAsyncPreferred();

    default boolean isExpirationDateSupported() {
        return false;
    }

    boolean exists(String path) throws IOException;
    CompletableFuture<Boolean> existsAsync(String path);

    long length(String path) throws IOException;
    CompletableFuture<Long> lengthAsync(String path);

    void testReadWrite() throws IOException;
    CompletableFuture<Void> testReadWriteAsync();

    default Binary readBinary(String path, StorageReadOptions options) throws IOException {
        return Binary.fromInputStream(readStream(path, options));
    }

    default CompletableFuture<Binary> readBinaryAsync(String path, StorageReadOptions options) {
        CompletableFuture<Binary> future = new CompletableFuture<>();
        readStreamAsync(path, options).whenComplete((in, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                try {
                    Binary binary = Binary.fromInputStream(in);
                    future.complete(binary);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            }
        });
        return future;
    }

    default byte[] readFully(String path, StorageReadOptions options) throws IOException {
        try (Binary binary = readBinary(path, options)) {
            return binary.getBytes();
        }
    }

    default CompletableFuture<byte[]> readFullyAsync(String path, StorageReadOptions options) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        readBinaryAsync(path, options).whenComplete((binary, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                try {
                    byte[] data = binary.getBytes();
                    future.complete(data);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            }
        });
        return future;
    }

    InputStream readStream(String path, StorageReadOptions options) throws IOException;
    CompletableFuture<InputStream> readStreamAsync(String path, StorageReadOptions options);

    void write(String path, Binary data, StorageWriteOptions options) throws IOException;
    CompletableFuture<Void> writeAsync(String path, Binary data, StorageWriteOptions options);

    void delete(String path) throws IOException;
    CompletableFuture<Void> deleteAsync(String path);
}
