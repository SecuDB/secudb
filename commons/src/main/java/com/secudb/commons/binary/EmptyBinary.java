package com.secudb.commons.binary;

import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class EmptyBinary extends AbstractBinary {
    public static final byte[] EMPTY_BYTES = new byte[0];

    @Override
    public boolean hasLength() {
        return true;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public boolean isSingleReadStream() {
        return false;
    }

    @Override
    public byte[] getBytes() {
        return EMPTY_BYTES;
    }

    @Override
    public byte[] getBytesReadOnly() {
        return EMPTY_BYTES;
    }

    @Override
    public long writeTo(OutputStream out) {
        return 0;
    }

    @Override
    public long writeTo(WritableByteChannel channel) {
        return 0;
    }

    @Override
    public long writeTo(FileChannel fileChannel, long position) {
        return 0;
    }

    @Override
    public Binary fragment(long offset, long length) {
        return EMPTY;
    }

    @Override
    public boolean isInMemory() {
        return true;
    }

    @Override
    public Binary toMemory() {
        return this;
    }
}
