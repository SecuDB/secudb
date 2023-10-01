package com.secudb.storage.api;

import com.secudb.commons.binary.Binary;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.*;

public abstract class AbstractSyncStorage implements Storage {

    protected final ExecutorService executor = new ThreadPoolExecutor(
            0,
            Runtime.getRuntime().availableProcessors() + 1,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<>());

    @Override
    public boolean isAsyncPreferred() {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> existsAsync(String path) {
        return asyncCallable(() -> exists(path));
    }

    @Override
    public CompletableFuture<Long> lengthAsync(String path) {
        return asyncCallable(() -> length(path));
    }

    @Override
    public void testReadWrite() throws IOException {
        String tempFilename = "test-" + UUID.randomUUID();
        String data1 = "1stPass-" + tempFilename;
        String data2 = "2ndPass-" + tempFilename;

        try {
            write(tempFilename, Binary.fromStringUTF8(data1), StorageWriteOptions.OVERWRITE);
            String storedData1 = readBinary(tempFilename, StorageReadOptions.DEFAULT).toStringUTF8();
            if (!storedData1.equals(data1)) {
                throw new IOException("Storage fatal error - inconsistent read after initial write!");
            }

            write(tempFilename, Binary.fromStringUTF8(data2), StorageWriteOptions.OVERWRITE);
            String storedData2 = readBinary(tempFilename, StorageReadOptions.DEFAULT).toStringUTF8();
            if (!storedData2.equals(data2)) {
                throw new IOException("Storage fatal error - inconsistent read after replacement write!");
            }
        } finally {
            try {
                delete(tempFilename);
            } catch (Exception ignore) { }
        }
    }

    @Override
    public CompletableFuture<Void> testReadWriteAsync() {
        return asyncRunnable(this::testReadWrite);
    }

    @Override
    public CompletableFuture<Binary> readBinaryAsync(String path, StorageReadOptions options) {
        return asyncCallable(() -> readBinary(path, options));
    }

    @Override
    public CompletableFuture<byte[]> readFullyAsync(String path, StorageReadOptions options) {
        return asyncCallable(() -> readFully(path, options));
    }

    @Override
    public CompletableFuture<InputStream> readStreamAsync(String path, StorageReadOptions options) {
        return asyncCallable(() -> readStream(path, options));
    }

    @Override
    public CompletableFuture<Void> writeAsync(String path, Binary data, StorageWriteOptions options) {
        return asyncRunnable(() -> write(path, data, options));
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        return asyncRunnable(() -> delete(path));
    }

    protected CompletableFuture<Void> asyncRunnable(RunnableThrowable runnable) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                runnable.run();
                future.complete(null);
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    protected <V> CompletableFuture<V> asyncCallable(Callable<V> callable) {
        CompletableFuture<V> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                V result = callable.call();
                future.complete(result);
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    private interface RunnableThrowable {
        void run() throws Exception;
    }
}
