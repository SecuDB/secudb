package com.secudb.storage.filesystem;

import com.secudb.commons.binary.Binary;
import com.secudb.commons.io.IOStreams;
import com.secudb.storage.api.AbstractSyncStorage;
import com.secudb.storage.api.StorageReadOptions;
import com.secudb.storage.api.StorageWriteOptions;
import com.secudb.storage.api.exceptions.AlreadyExistsInStorageException;
import com.secudb.storage.api.exceptions.NotFoundInStorageException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.util.UUID;

public class FileSystemStorage extends AbstractSyncStorage {
    private final String name;

    protected final Path rootPath;

    public FileSystemStorage(String name, Path rootPath) {
        this.name = name != null && !name.isEmpty() ? name : rootPath.toString();
        this.rootPath = rootPath;
    }

    @Override
    public String getName() {
        return name;
    }

    protected Path resolve(String path) {
        return rootPath.resolve(path);
    }

    @Override
    public boolean exists(String path) throws IOException {
        return Files.exists(resolve(path));
    }

    @Override
    public long length(String path) throws IOException {
        return Files.size(resolve(path));
    }

    @Override
    public void write(String path, Binary data, StorageWriteOptions options) throws AlreadyExistsInStorageException, IOException {
        Path targetFilePath = resolve(path);
        if (!options.isAllowOverwrite() && Files.exists(targetFilePath)) {
            throw new AlreadyExistsInStorageException();
        }

        Path parentPath = targetFilePath.getParent();
        if (!Files.exists(parentPath)) {
            Files.createDirectories(parentPath);
        }


        Path tempFilePath = parentPath.resolve(".~" + targetFilePath.getFileName() + "." + UUID.randomUUID() + ".tmp");
        try {
            try (InputStream in = data.toInputStream()) {
                try (OutputStream out = Files.newOutputStream(tempFilePath, StandardOpenOption.CREATE_NEW)) {
                    IOStreams.transfer(in, out);
                }
            }

            try {
                try {
                    if (options.isAllowOverwrite()) {
                        Files.move(tempFilePath, targetFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                    } else {
                        Files.move(tempFilePath, targetFilePath, StandardCopyOption.ATOMIC_MOVE);
                    }
                } catch (AccessDeniedException | AtomicMoveNotSupportedException e) {
                    if (options.isAllowOverwrite()) {
                        Files.move(tempFilePath, targetFilePath, StandardCopyOption.REPLACE_EXISTING);
                    } else {
                        Files.move(tempFilePath, targetFilePath);
                    }
                }
            } catch (FileAlreadyExistsException e) {
                throw new AlreadyExistsInStorageException(e);
            }
        } finally {
            Files.deleteIfExists(tempFilePath);
        }
    }

    @Override
    public InputStream readStream(String path, StorageReadOptions options) throws NotFoundInStorageException, IOException {
        try {
            InputStream inputStream = Files.newInputStream(resolve(path), StandardOpenOption.READ);
            return options.applyOffsetAndLength(inputStream);
        } catch (NoSuchFileException e) {
            throw new NotFoundInStorageException(e);
        }
    }

    @Override
    public void delete(String path) throws NotFoundInStorageException, IOException {
        try {
            Files.delete(Paths.get(path));
        } catch (NoSuchFileException e) {
            throw new NotFoundInStorageException(e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
