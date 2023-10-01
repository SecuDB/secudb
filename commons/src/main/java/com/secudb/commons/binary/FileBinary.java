package com.secudb.commons.binary;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileBinary extends FileChannelBinary {
    private Path path;

    public FileBinary(Path path) {
        super(prepareFileChannel(path));
        this.path = path;
    }

    public FileBinary(Path path, long position, long length) {
        super(prepareFileChannel(path), position, length);
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    static FileChannel prepareFileChannel(Path path) {
        try {
            return FileChannel.open(path, StandardOpenOption.READ);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
